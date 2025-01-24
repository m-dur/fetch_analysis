import json
import psycopg2
from pathlib import Path
from typing import Dict, List, Any
import sys
from datetime import datetime
import os

SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent
DATA_DIR = PROJECT_ROOT / 'raw_data'

class DatabaseIngester:
    def __init__(self, dbname="fetchdb", user="guest", password="Password123!", host="localhost", port="5432"):
        self.conn = psycopg2.connect(
            dbname=dbname, user=user, password=password, host=host, port=port
        )
        self.cur = self.conn.cursor()

    def close(self):
        self.cur.close()
        self.conn.close()

    def read_json_file(self, file_name: str) -> List[Dict]:
        """Read JSON Lines file from raw_data directory"""
        file_path = DATA_DIR / file_name
        data = []
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                for line in file:
                    if line.strip():  
                        try:
                            item = json.loads(line.strip())
                            data.append(item)
                        except json.JSONDecodeError as e:
                            print(f"Error parsing line in {file_name}: {str(e)}")
                            continue
            return data
        except Exception as e:
            print(f"Error reading file {file_path}: {str(e)}")
            return []

    def process_timestamp(self, value: Any) -> Any:
        """Convert MongoDB date format to PostgreSQL timestamp"""
        if isinstance(value, dict) and '$date' in value:
            return datetime.fromtimestamp(value['$date'] / 1000)
        return value

    def insert_brands_and_categories(self, brands_data: List[Dict]):
        """Insert brands and their categories, maintaining referential integrity"""
        categories = {brand.get('category', 'UNKNOWN'): brand.get('categoryCode', '') 
                     for brand in brands_data if brand.get('category')}
        
        for category, categorycode in categories.items():
            try:
                self.cur.execute("""
                    INSERT INTO categories (category, categorycode)
                    VALUES (%s, %s)
                    ON CONFLICT (category) DO UPDATE 
                    SET categorycode = EXCLUDED.categorycode
                    RETURNING categories_id
                """, (category, categorycode))
                self.conn.commit()
            except Exception as e:
                print(f"Error inserting category {category}: {str(e)}")
                self.conn.rollback()

        for brand in brands_data:
            try:
                category = brand.get('category', 'UNKNOWN')
                self.cur.execute(
                    "SELECT categories_id FROM categories WHERE category = %s",
                    (category,)
                )
                result = self.cur.fetchone()
                if result:
                    category_id = result[0]
                    self.cur.execute("""
                        INSERT INTO brands (_id, barcode, name, topbrand, brandcode, category_id)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (brandcode) DO NOTHING
                    """, (
                        brand.get('_id', {}).get('$oid'),
                        brand.get('barcode'),
                        brand.get('name'),
                        brand.get('topBrand', False),
                        brand.get('brandCode'),
                        category_id
                    ))
                    self.conn.commit()
            except Exception as e:
                print(f"Error inserting brand {brand.get('name')}: {str(e)}")
                self.conn.rollback()

    def insert_users_and_receipts(self, users_data: List[Dict], receipts_data: List[Dict]):
        """Insert users first, then receipts with proper foreign key handling"""
        for user in users_data:
            try:
                self.cur.execute("""
                    INSERT INTO users (_id, active, createddate, lastlogin, role, signupsource, state)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (_id) DO NOTHING
                """, (
                    user.get('_id', {}).get('$oid'),
                    user.get('active'),
                    self.process_timestamp(user.get('createdDate')),
                    self.process_timestamp(user.get('lastLogin')),
                    user.get('role'),
                    user.get('signUpSource'),
                    user.get('state')
                ))
                self.conn.commit()
            except Exception as e:
                print(f"Error inserting user: {str(e)}")
                self.conn.rollback()

        for receipt in receipts_data:
            try:
                user_id = receipt.get('userId', {}).get('$oid')
                if not user_id:
                    continue

                self.cur.execute(
                    "SELECT _id FROM users WHERE _id = %s",
                    (user_id,)
                )
                if self.cur.fetchone():
                    # User exists, insert receipt
                    self.insert_receipt_without_items(receipt)
            except Exception as e:
                print(f"Error processing receipt: {str(e)}")
                self.conn.rollback()

    def track_missing_brand(self, brandcode: str):
        """Track brands that exist in receipts but not in brands table"""
        try:
            self.cur.execute("""
                INSERT INTO missing_brands (brandcode, occurrence_count)
                VALUES (%s, 1)
                ON CONFLICT (brandcode) 
                DO UPDATE SET 
                    occurrence_count = missing_brands.occurrence_count + 1,
                    last_seen = CURRENT_TIMESTAMP
            """, (brandcode,))
            self.conn.commit()
        except Exception as e:
            print(f"Error tracking missing brand {brandcode}: {str(e)}")
            self.conn.rollback()

    def track_missing_user(self, user_id: str):
        """Track users that exist in receipts but not in users table"""
        try:
            self.cur.execute("""
                INSERT INTO missing_users (user_id, occurrence_count)
                VALUES (%s, 1)
                ON CONFLICT (user_id) 
                DO UPDATE SET 
                    occurrence_count = missing_users.occurrence_count + 1,
                    last_seen = CURRENT_TIMESTAMP
            """, (user_id,))
            self.conn.commit()
        except Exception as e:
            print(f"Error tracking missing user {user_id}: {str(e)}")
            self.conn.rollback()

    def insert_receipt_without_items(self, receipt: Dict):
        """Insert just the receipt without its items"""
        try:
            user_id = self.process_value(receipt.get('userId'))
            if user_id:
                self.cur.execute(
                    "SELECT _id FROM users WHERE _id = %s",
                    (user_id,)
                )
                if not self.cur.fetchone():
                    print(f"Warning: User {user_id} not found - setting to NULL")
                    self.track_missing_user(user_id)
                    user_id = None

            receipt_values = [
                self.process_value(receipt.get('_id')),
                receipt.get('bonusPointsEarned'),
                receipt.get('bonusPointsEarnedReason'),
                self.process_value(receipt.get('createDate')),
                self.process_value(receipt.get('dateScanned')),
                self.process_value(receipt.get('finishedDate')),
                self.process_value(receipt.get('modifyDate')),
                self.process_value(receipt.get('pointsAwardedDate')),
                receipt.get('pointsEarned'),
                self.process_value(receipt.get('purchaseDate')),
                receipt.get('purchasedItemCount'),
                receipt.get('rewardsReceiptStatus'),
                receipt.get('totalSpent'),
                user_id  
            ]

            self.cur.execute("""
                INSERT INTO receipts (
                    _id, bonuspointsearned, bonuspointsearnedreason,
                    createdate, datescanned, finisheddate, modifydate,
                    pointsawardeddate, pointsearned, purchasedate,
                    purchaseditemcount, rewardsreceiptstatus,
                    totalspent, userid
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING receipts_id
            """, receipt_values)
            
            self.conn.commit()
            return self.cur.fetchone()[0]
        except Exception as e:
            print(f"Error inserting receipt: {str(e)}")
            self.conn.rollback()
            return None

    def insert_receipt_items(self, receipt: Dict, receipt_id: int):
        """Insert receipt items for a given receipt"""
        for item in receipt.get('rewardsReceiptItemList', []):
            try:
                brandcode = item.get('brandCode')
                if brandcode:
                    self.cur.execute(
                        "SELECT brandcode FROM brands WHERE brandcode = %s",
                        (brandcode,)
                    )
                    if not self.cur.fetchone():
                        self.track_missing_brand(brandcode)
                
                item_values = [
                    item.get('barcode'),
                    item.get('description'),
                    item.get('finalPrice'),
                    item.get('itemPrice'),
                    item.get('needsFetchReview'),
                    item.get('partnerItemId'),
                    item.get('preventTargetGapPoints'),
                    item.get('quantityPurchased'),
                    item.get('userFlaggedBarcode'),
                    item.get('userFlaggedNewItem'),
                    item.get('userFlaggedPrice'),
                    item.get('userFlaggedQuantity'),
                    item.get('originalMetaBriteBarcode'),
                    item.get('originalMetaBriteDescription'),
                    item.get('pointsNotAwardedReason'),
                    item.get('pointsPayerId'),
                    item.get('rewardsGroup'),
                    item.get('rewardsProductPartnerId'),
                    brandcode,  
                    item.get('competitorRewardsGroup'),
                    item.get('discountedItemPrice'),
                    item.get('originalReceiptItemText'),
                    item.get('itemNumber'),
                    item.get('needsFetchReviewReason'),
                    item.get('originalMetaBriteQuantityPurchased'),
                    item.get('pointsEarned'),
                    item.get('targetPrice'),
                    item.get('competitiveProduct'),
                    item.get('userFlaggedDescription'),
                    item.get('deleted'),
                    item.get('priceAfterCoupon'),
                    item.get('metabriteCampaignId'),
                    receipt_id
                ]

                self.cur.execute("""
                    INSERT INTO rewardsreceiptitemlist (
                        barcode, description, finalprice, itemprice,
                        needsfetchreview, partneritemid, preventtargetgappoints,
                        quantitypurchased, userflaggedbarcode, userflaggednewitem,
                        userflaggedprice, userflaggedquantity, originalmetabritebarcode,
                        originalmetabritedescription, pointsnotawardedreason,
                        pointspayerid, rewardsgroup, rewardsproductpartnerid,
                        brandcode, competitorrewardsgroup, discounteditemprice,
                        originalreceiptitemtext, itemnumber, needsfetchreviewreason,
                        originalmetabritequantitypurchased, pointsearned,
                        targetprice, competitiveproduct, userflaggeddescription,
                        deleted, priceaftercoupon, metabritecampaignid, receipt_id
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s)
                """, item_values)
                self.conn.commit()
            except Exception as e:
                print(f"Error inserting receipt item: {str(e)}")
                self.conn.rollback()

    def process_value(self, value):
        """Handle potential null values and MongoDB objects"""
        if isinstance(value, dict):
            if '$date' in value:
                return self.process_timestamp(value)
            if '$oid' in value:
                return value.get('$oid')
        return value

def main():
    if len(sys.argv) < 2:
        print("Usage: python data_ingester.py <json_file1> [json_file2] [json_file3] ...")
        print("\nAvailable JSON files in raw_data:")
        for json_file in DATA_DIR.glob('*.json'):
            print(f"- {json_file.name}")
        sys.exit(1)

    ingester = DatabaseIngester()
    
    try:
        data_store = {}
        for file_name in sys.argv[1:]:
            print(f"\nReading: {file_name}")
            file_stem = Path(file_name).stem
            data = ingester.read_json_file(file_name)
            if data:
                data_store[file_stem] = data

        if 'users' in data_store and 'receipts' in data_store:
            user_ids = set(user['_id'].get('$oid') for user in data_store['users'])
            
            receipt_user_ids = set()
            for receipt in data_store['receipts']:
                user_id = receipt.get('userId')
                if isinstance(user_id, dict):
                    user_id = user_id.get('$oid')
                receipt_user_ids.add(user_id)
            
            missing_users = receipt_user_ids - user_ids
            
            print("\nData Analysis:")
            print(f"Total unique users in users.json: {len(user_ids)}")
            print(f"Total unique users in receipts: {len(receipt_user_ids)}")
            print(f"Users missing from users.json: {len(missing_users)}")
            print("\nFirst 5 missing user IDs as examples:")
            for user_id in list(missing_users)[:5]:
                print(f"- {user_id}")

        # 1. Categories and Brands
        if 'brands' in data_store:
            print("\nProcessing: categories and brands")
            ingester.insert_brands_and_categories(data_store['brands'])

        # 2. Users
        if 'users' in data_store:
            print("\nProcessing: users")
            ingester.insert_users_and_receipts(data_store['users'], [])

        # 3. Receipts (without items first)
        if 'receipts' in data_store:
            print("\nProcessing: receipts")
            total_receipts = len(data_store['receipts'])
            skipped_receipts = 0
            receipts_with_ids = []
            
            for receipt in data_store['receipts']:
                receipt_id = ingester.insert_receipt_without_items(receipt)
                if receipt_id:
                    receipts_with_ids.append((receipt, receipt_id))
                else:
                    skipped_receipts += 1

            print(f"\nReceipts summary:")
            print(f"Total receipts: {total_receipts}")
            print(f"Successfully inserted: {len(receipts_with_ids)}")
            print(f"Skipped: {skipped_receipts}")

        # 4. Receipt Items (only after all receipts are inserted)
        if 'receipts' in data_store:
            print("\nProcessing: receipt items")
            for receipt, receipt_id in receipts_with_ids:
                ingester.insert_receipt_items(receipt, receipt_id)

        print("\nException Summary:")
        ingester.cur.execute("SELECT COUNT(*) FROM missing_brands")
        missing_brands_count = ingester.cur.fetchone()[0]
        print(f"Unique missing brands: {missing_brands_count}")

        ingester.cur.execute("SELECT COUNT(*) FROM missing_users")
        missing_users_count = ingester.cur.fetchone()[0]
        print(f"Unique missing users: {missing_users_count}")

        # Show top 5 most frequent missing brands
        print("\nTop 5 most frequent missing brands:")
        ingester.cur.execute("""
            SELECT brandcode, occurrence_count 
            FROM missing_brands 
            ORDER BY occurrence_count DESC 
            LIMIT 5
        """)
        for brandcode, count in ingester.cur.fetchall():
            print(f"  {brandcode}: {count} occurrences")

        # Show top 5 most frequent missing users
        print("\nTop 5 most frequent missing users:")
        ingester.cur.execute("""
            SELECT user_id, occurrence_count 
            FROM missing_users 
            ORDER BY occurrence_count DESC 
            LIMIT 5
        """)
        for user_id, count in ingester.cur.fetchall():
            print(f"  {user_id}: {count} occurrences")

    finally:
        ingester.close()

if __name__ == "__main__":
    main()
