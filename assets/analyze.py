import json
from datetime import datetime, timedelta
from collections import Counter, defaultdict
from pathlib import Path
import os
from typing import List, Dict, Any
import pandas as pd

SCRIPT_DIR = Path(__file__).parent
DATA_DIR = SCRIPT_DIR.parent / 'raw_data'
RECEIPTS_FILE = DATA_DIR / 'receipts.json'
USERS_FILE = DATA_DIR / 'users.json'
BRANDS_FILE = DATA_DIR / 'brands.json'

print("Current working directory:", os.getcwd())
print("Attempting to open:", os.path.abspath('../raw_data/receipts.json'))

def load_json_lines(file_path: Path) -> List[Dict[Any, Any]]:
    """Load JSON Lines file into a list of dictionaries."""
    data = []
    with open(file_path, 'r') as file:
        for line in file:
            try:
                data.append(json.loads(line.strip()))
            except json.JSONDecodeError as e:
                print(f"Error parsing line in {file_path.name}: {e}")
    return data

def profile_data_quality(data: List[dict], entity_name: str) -> None:
    """Profile data quality issues in the dataset."""
    print(f"\n=== Data Quality Profile for {entity_name} ===")
    
    # Count total records
    total_records = len(data)
    print(f"Total records: {total_records}")
    
    if not data:
        print("No data to analyze!")
        return
    
    # Analyze field completeness
    fields = set().union(*(d.keys() for d in data))
    print("\nField completeness:")
    for field in sorted(fields):
        non_null_count = sum(1 for d in data if field in d and d[field] is not None)
        null_count = total_records - non_null_count
        null_percentage = (null_count / total_records) * 100
        print(f"  {field}: {null_percentage:.1f}% null ({null_count} records)")
        
        # Sample unique values for categorical fields
        if field in ['rewardsReceiptStatus', 'role', 'state', 'category']:
            unique_values = set(d.get(field) for d in data if field in d and d[field] is not None)
            print(f"    Unique values: {sorted(unique_values)}")

def analyze_business_questions(receipts: List[dict], users: List[dict], brands: List[dict]) -> None:
    """Analyze data for business questions."""
    print("\n=== Business Questions Analysis ===")
    
    # Handle date fields that might be dictionaries
    def safe_parse_date(date_value):
        if isinstance(date_value, dict):
            # Handle MongoDB-style dates that contain milliseconds since epoch
            if '$date' in date_value:
                try:
                    # Convert milliseconds to seconds by dividing by 1000
                    timestamp = date_value['$date'] / 1000
                    return pd.to_datetime(timestamp, unit='s')
                except (ValueError, TypeError):
                    return None
        try:
            return pd.to_datetime(date_value)
        except:
            return None
    
    def get_id(doc):
        id_value = doc.get('_id')
        if isinstance(id_value, dict):
            return str(id_value.get('$oid', id_value))
        return str(id_value)
    
    # 1. Top 5 brands by receipts scanned for most recent month AND previous month comparison
    receipt_dates = [safe_parse_date(receipt.get('dateScanned')) for receipt in receipts]
    valid_dates = [d for d in receipt_dates if d is not None]
    latest_month = max(valid_dates).replace(day=1) if valid_dates else None
    previous_month = (latest_month - timedelta(days=1)).replace(day=1) if latest_month else None
    
    print("\nDate Range Analysis:")
    print(f"Latest date in data: {max(valid_dates) if valid_dates else 'No valid dates'}")
    print(f"Latest month: {latest_month}")
    print(f"Previous month: {previous_month}")
    
    current_month_brands = defaultdict(int)
    previous_month_brands = defaultdict(int)
    
    receipt_count = 0
    brand_code_count = 0
    
    for receipt, scan_date in zip(receipts, receipt_dates):
        if not scan_date:
            continue
            
        scan_month = scan_date.replace(day=1)
        if scan_month == latest_month:
            receipt_count += 1
            for item in receipt.get('rewardsReceiptItemList', []):
                brand_code = item.get('brandCode')
                if brand_code:
                    brand_code_count += 1
                    current_month_brands[brand_code] += 1
        elif scan_month == previous_month:
            for item in receipt.get('rewardsReceiptItemList', []):
                brand_code = item.get('brandCode')
                if brand_code:
                    previous_month_brands[brand_code] += 1
    
    print(f"\nReceipts in latest month: {receipt_count}")
    print(f"Items with brand codes in latest month: {brand_code_count}")
    print(f"Unique brands in latest month: {len(current_month_brands)}")
    print(f"Unique brands in previous month: {len(previous_month_brands)}")
    
    print("\n1. Top 5 brands for most recent month:")
    current_top_5 = sorted(current_month_brands.items(), key=lambda x: x[1], reverse=True)[:5]
    if current_top_5:
        for brand_code, count in current_top_5:
            brand_name = next((b['name'] for b in brands if b.get('brandCode') == brand_code), brand_code)
            print(f"  {brand_name}: {count} receipts")
    else:
        print("  No brand data available for the most recent month")
    
    print("\n2. Month-over-month comparison:")
    if current_top_5:
        for brand_code, current_count in current_top_5:
            brand_name = next((b['name'] for b in brands if b.get('brandCode') == brand_code), brand_code)
            previous_count = previous_month_brands[brand_code]
            change = current_count - previous_count
            print(f"  {brand_name}: Current: {current_count}, Previous: {previous_count}, Change: {change:+d}")
    else:
        print("  No brand data available for comparison")
    
    # 3. Analysis by receipt status (Accepted/Rejected)
    status_metrics = defaultdict(lambda: {'spend': 0.0, 'items': 0, 'count': 0})
    
    for receipt in receipts:
        status = receipt.get('rewardsReceiptStatus')
        if status in ['ACCEPTED', 'REJECTED']:
            try:
                spent = float(receipt.get('totalSpent', 0))
                items = int(receipt.get('purchasedItemCount', 0))
                status_metrics[status]['spend'] += spent
                status_metrics[status]['items'] += items
                status_metrics[status]['count'] += 1
            except (ValueError, TypeError):
                continue
    
    print("\n3. Receipt Status Analysis (Accepted vs Rejected):")
    for status, metrics in status_metrics.items():
        avg_spend = metrics['spend'] / metrics['count'] if metrics['count'] > 0 else 0
        avg_items = metrics['items'] / metrics['count'] if metrics['count'] > 0 else 0
        print(f"  {status}:")
        print(f"    Average spend: ${avg_spend:.2f}")
        print(f"    Average items: {avg_items:.1f}")
        print(f"    Total receipts: {metrics['count']}")
    
    # 4 & 5. Brand analysis for recent users
    six_months_ago = max(valid_dates) - timedelta(days=180) if valid_dates else None
    if six_months_ago:
        # Parse user creation dates first
        user_creation_dates = {
            get_id(u): safe_parse_date(u.get('createdDate'))
            for u in users
        }
        
        recent_user_ids = {
            user_id
            for user_id, created_date in user_creation_dates.items()
            if created_date and created_date >= six_months_ago
        }
        
        brand_metrics = defaultdict(lambda: {
            'spend': 0.0, 
            'transactions': set(),  
            'total_items': 0,
            'total_spend': 0.0
        })
        
        for receipt in receipts:
            receipt_user_id = get_id({'_id': receipt.get('userId')})
            if receipt_user_id in recent_user_ids:
                receipt_id = get_id(receipt)  
                for item in receipt.get('rewardsReceiptItemList', []):
                    brand_code = item.get('brandCode')
                    if brand_code:
                        try:
                            quantity = int(item.get('quantityPurchased', 1))
                            price = float(item.get('finalPrice', 0))
                            brand_metrics[brand_code]['total_spend'] += price * quantity
                            brand_metrics[brand_code]['total_items'] += quantity
                            brand_metrics[brand_code]['transactions'].add(receipt_id)  
                        except (ValueError, TypeError):
                            continue
        
        print("\n4. Brands with highest spend among recent users (past 6 months):")
        brand_results = []
        for brand_code, metrics in brand_metrics.items():
            brand_name = next((b['name'] for b in brands if b.get('brandCode') == brand_code), 
                            f"missing_brand_{brand_code}")
            brand_status = "valid brand" if any(b.get('brandCode') == brand_code for b in brands) else "missing from brands table"
            
            avg_price = (metrics['total_spend'] / metrics['total_items'] 
                       if metrics['total_items'] > 0 else 0)
            
            brand_results.append({
                'brand_name': brand_name,
                'total_spend': metrics['total_spend'],
                'total_items': metrics['total_items'],
                'transaction_count': len(metrics['transactions']),
                'avg_price_per_item': round(avg_price, 2),
                'brand_status': brand_status
            })
        
        # Sort by total spend and display top 6
        sorted_by_spend = sorted(brand_results, key=lambda x: x['total_spend'], reverse=True)[:6]
        for brand in sorted_by_spend:
            print(f"\n  {brand['brand_name']}:")
            print(f"    Total spend: ${brand['total_spend']:.2f}")
            print(f"    Total items: {brand['total_items']}")
            print(f"    Transaction count: {brand['transaction_count']}")
            print(f"    Avg price per item: ${brand['avg_price_per_item']:.2f}")
            print(f"    Status: {brand['brand_status']}")

        print("\n5. Brands with most transactions among recent users (past 6 months):")
        sorted_by_transactions = sorted(brand_results, key=lambda x: x['transaction_count'], reverse=True)[:6]
        for brand in sorted_by_transactions:
            print(f"\n  {brand['brand_name']}:")
            print(f"    Transaction count: {brand['transaction_count']}")
            print(f"    Total spend: ${brand['total_spend']:.2f}")
            print(f"    Total items: {brand['total_items']}")
            print(f"    Avg price per item: ${brand['avg_price_per_item']:.2f}")
            print(f"    Status: {brand['brand_status']}")
    else:
        print("  No data available for recent users")

def analyze_brand_codes(receipts: List[dict], brands: List[dict]) -> None:
    """Analyze brand codes in receipts and brands data"""
    print("\n=== Brand Code Analysis ===")
    
    brand_codes_from_brands = {
        brand.get('brandCode'): brand.get('name', 'Unknown')
        for brand in brands 
        if brand.get('brandCode')
    }
    

    brand_codes_from_receipts = set()
    brand_code_frequency = defaultdict(int)
    total_items = 0
    
    for receipt in receipts:
        for item in receipt.get('rewardsReceiptItemList', []):
            total_items += 1
            brand_code = item.get('brandCode')
            if brand_code:
                brand_codes_from_receipts.add(brand_code)
                brand_code_frequency[brand_code] += 1
    
    # Analysis results
    brands_only_in_receipts = brand_codes_from_receipts - set(brand_codes_from_brands.keys())
    brands_only_in_brands = set(brand_codes_from_brands.keys()) - brand_codes_from_receipts
    brands_in_both = brand_codes_from_receipts & set(brand_codes_from_brands.keys())
    
    print("\nBrand Code Statistics:")
    print(f"Total unique brand codes in brands table: {len(brand_codes_from_brands)}")
    print(f"Total unique brand codes in receipts: {len(brand_codes_from_receipts)}")
    print(f"Total receipt items: {total_items}")
    print(f"Receipt items with brand codes: {sum(brand_code_frequency.values())}")
    print(f"Receipt items without brand codes: {total_items - sum(brand_code_frequency.values())}")
    
    print("\nBrand Code Overlap:")
    print(f"Brand codes in both: {len(brands_in_both)}")
    print(f"Brand codes only in receipts: {len(brands_only_in_receipts)}")
    print(f"Brand codes only in brands table: {len(brands_only_in_brands)}")
    
    # Show top 10 most frequent brand codes in receipts
    print("\nTop 10 most frequent brand codes in receipts:")
    top_brands = sorted(brand_code_frequency.items(), key=lambda x: x[1], reverse=True)[:10]
    for brand_code, frequency in top_brands:
        brand_name = brand_codes_from_brands.get(brand_code, "NOT IN BRANDS TABLE")
        print(f"  {brand_code} ({brand_name}): {frequency} occurrences")
    
    # Show sample of missing brands
    if brands_only_in_receipts:
        print("\nSample of brand codes in receipts but missing from brands table (up to 10):")
        sample_missing = list(brands_only_in_receipts)[:10]
        for brand_code in sample_missing:
            frequency = brand_code_frequency[brand_code]
            print(f"  {brand_code}: {frequency} occurrences")
    
    # Data quality implications
    print("\nData Quality Implications:")
    missing_brands_items = sum(brand_code_frequency[code] for code in brands_only_in_receipts)
    if missing_brands_items > 0:
        print(f"- {missing_brands_items} receipt items ({(missing_brands_items/total_items*100):.1f}%) " 
              f"reference brand codes that don't exist in the brands table")
    
    unused_brands = len(brands_only_in_brands)
    if unused_brands > 0:
        print(f"- {unused_brands} brands ({(unused_brands/len(brand_codes_from_brands)*100):.1f}%) " 
              f"in the brands table are never referenced in receipts")

def get_mongo_id(doc: dict) -> str:
    """Extract ID from MongoDB-style document, handling both string and dict formats."""
    id_value = doc.get('_id')
    if isinstance(id_value, dict) and '$oid' in id_value:
        return id_value['$oid']
    return str(id_value) if id_value else None

def analyze_receipt_status(receipts: List[dict], users: List[dict]) -> None:
    """Analyze receipts by status (Accepted/Rejected)"""
    print("\n=== Receipt Status Analysis ===")
    
    status_metrics = defaultdict(lambda: {
        'count': 0,
        'total_spent': 0.0,
        'total_items': 0,
        'missing_users': set()
    })
    
    user_ids = {get_mongo_id(u): u for u in users if get_mongo_id(u)}
    
    for receipt in receipts:
        status = receipt.get('rewardsReceiptStatus', '').lower()
        user_id = get_mongo_id(receipt) 
        
        try:
            status_metrics[status]['count'] += 1
            status_metrics[status]['total_spent'] += float(receipt.get('totalSpent', 0))
            status_metrics[status]['total_items'] += int(receipt.get('purchasedItemCount', 0))
            
            if user_id and user_id not in user_ids:
                status_metrics[status]['missing_users'].add(user_id)
                
        except (ValueError, TypeError):
            continue
    
    print("\nStatus Metrics:")
    for status, metrics in status_metrics.items():
        if status in ['accepted', 'rejected']:
            avg_spend = metrics['total_spent'] / metrics['count'] if metrics['count'] > 0 else 0
            avg_items = metrics['total_items'] / metrics['count'] if metrics['count'] > 0 else 0
            print(f"\n{status.upper()}:")
            print(f"  Receipt count: {metrics['count']}")
            print(f"  Average spend: ${avg_spend:.2f}")
            print(f"  Average items: {avg_items:.1f}")
            print(f"  Missing users: {len(metrics['missing_users'])}")

def analyze_data_quality(receipts: List[dict], users: List[dict], brands: List[dict]) -> None:
    """Comprehensive data quality analysis"""
    print("\n=== Data Quality Analysis ===")
    
    receipt_issues = defaultdict(int)
    for receipt in receipts:
        if not receipt.get('_id'):
            receipt_issues['missing_receipt_id'] += 1
        if not receipt.get('userId'):
            receipt_issues['missing_user_id'] += 1
        if not receipt.get('dateScanned'):
            receipt_issues['missing_scan_date'] += 1
            
        try:
            pd.to_datetime(receipt.get('dateScanned'))
        except:
            receipt_issues['invalid_scan_date'] += 1
            
        try:
            if float(receipt.get('totalSpent', 0)) < 0:
                receipt_issues['negative_total_spent'] += 1
            if int(receipt.get('purchasedItemCount', 0)) < 0:
                receipt_issues['negative_item_count'] += 1
        except (ValueError, TypeError):
            receipt_issues['invalid_numeric_values'] += 1
            
        items = receipt.get('rewardsReceiptItemList', [])
        for item in items:
            if not item.get('brandCode'):
                receipt_issues['missing_brand_codes'] += 1
            try:
                if float(item.get('finalPrice', 0)) < 0:
                    receipt_issues['negative_item_prices'] += 1
            except (ValueError, TypeError):
                receipt_issues['invalid_item_prices'] += 1
    
    print("\nReceipt Quality Issues:")
    for issue, count in receipt_issues.items():
        print(f"  {issue}: {count}")
    
    brand_codes = {b.get('brandCode') for b in brands if b.get('brandCode')}
    receipt_brand_codes = set()
    invalid_brand_refs = 0
    
    for receipt in receipts:
        for item in receipt.get('rewardsReceiptItemList', []):
            brand_code = item.get('brandCode')
            if brand_code:
                receipt_brand_codes.add(brand_code)
                if brand_code not in brand_codes:
                    invalid_brand_refs += 1
    
    print("\nBrand Reference Integrity:")
    print(f"  Total unique brands: {len(brand_codes)}")
    print(f"  Brands referenced in receipts: {len(receipt_brand_codes)}")
    print(f"  Invalid brand references: {invalid_brand_refs}")
    
    user_ids = {get_mongo_id(u) for u in users if get_mongo_id(u)}
    receipt_user_ids = {get_mongo_id({'_id': r.get('userId')}) for r in receipts if r.get('userId')}
    invalid_user_refs = len(receipt_user_ids - user_ids)
    
    print("\nUser Reference Integrity:")
    print(f"  Total users: {len(user_ids)}")
    print(f"  Users referenced in receipts: {len(receipt_user_ids)}")
    print(f"  Invalid user references: {invalid_user_refs}")

    print("\nAdditional Quality Metrics:")
    print(f"  Receipts with no items: {sum(1 for r in receipts if not r.get('rewardsReceiptItemList'))}")
    print(f"  Users with no receipts: {len(user_ids - receipt_user_ids)}")
    print(f"  Unused brands: {len(brand_codes - receipt_brand_codes)}")
    
    valid_dates = []
    for receipt in receipts:
        try:
            date = pd.to_datetime(receipt.get('dateScanned'))
            valid_dates.append(date)
        except:
            continue
    
    if valid_dates:
        print("\nDate Range Analysis:")
        print(f"  Earliest receipt: {min(valid_dates)}")
        print(f"  Latest receipt: {max(valid_dates)}")
        print(f"  Date range: {(max(valid_dates) - min(valid_dates)).days} days")

def main():
    print("Loading data...")
    receipts = load_json_lines(RECEIPTS_FILE)
    users = load_json_lines(USERS_FILE)
    brands = load_json_lines(BRANDS_FILE)
    
    # Run all analyses
    profile_data_quality(receipts, "Receipts")
    profile_data_quality(users, "Users")
    profile_data_quality(brands, "Brands")
    
    analyze_brand_codes(receipts, brands)
    analyze_receipt_status(receipts, users)
    analyze_data_quality(receipts, users, brands)
    analyze_business_questions(receipts, users, brands)

if __name__ == "__main__":
    main()