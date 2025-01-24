# Fetch Rewards Data Analysis

## Entity Relationship Diagram
See [ERD](Diagrams/ERD.md) for the database schema visualization.

## Database Schema
```sql
-- Categories table
CREATE TABLE categories (
    categories_id SERIAL PRIMARY KEY,
    category VARCHAR(255) UNIQUE,
    categorycode VARCHAR(255)
);

-- Brands table
CREATE TABLE brands (
    brands_id SERIAL PRIMARY KEY,
    _id VARCHAR(24),
    barcode VARCHAR(255),
    name VARCHAR(255),
    topbrand BOOLEAN,
    brandcode VARCHAR(255) UNIQUE,
    category_id INTEGER REFERENCES categories(categories_id) NULL
);

-- Users table
CREATE TABLE users (
    users_id SERIAL PRIMARY KEY,
    _id VARCHAR(24) UNIQUE,
    active BOOLEAN,
    createddate TIMESTAMP,
    lastlogin TIMESTAMP,
    role VARCHAR(255),
    signupsource VARCHAR(255),
    state VARCHAR(255)
);

-- Receipts table
CREATE TABLE receipts (
    receipts_id SERIAL PRIMARY KEY,
    _id VARCHAR(24),
    bonuspointsearned INTEGER,
    bonuspointsearnedreason VARCHAR(255),
    createdate TIMESTAMP,
    datescanned TIMESTAMP,
    finisheddate TIMESTAMP,
    modifydate TIMESTAMP,
    pointsawardeddate TIMESTAMP,
    pointsearned VARCHAR(255),
    purchasedate TIMESTAMP,
    purchaseditemcount INTEGER,
    rewardsreceiptstatus VARCHAR(255),
    totalspent DECIMAL,
    userid VARCHAR(24) REFERENCES users(_id) NULL
);

-- RewardsReceiptItemList table
CREATE TABLE rewardsreceiptitemlist (
    rewardsreceiptitemlist_id SERIAL PRIMARY KEY,
    barcode VARCHAR(255),
    description VARCHAR(255),
    finalprice DECIMAL,
    itemprice DECIMAL,
    needsfetchreview BOOLEAN,
    partneritemid VARCHAR(255),
    preventtargetgappoints BOOLEAN,
    quantitypurchased INTEGER,
    userflaggedbarcode VARCHAR(255),
    userflaggednewitem BOOLEAN,
    userflaggedprice DECIMAL,
    userflaggedquantity INTEGER,
    originalmetabritebarcode VARCHAR(255),
    originalmetabritedescription VARCHAR(255),
    pointsnotawardedreason VARCHAR(255),
    pointspayerid VARCHAR(255),
    rewardsgroup VARCHAR(255),
    rewardsproductpartnerid VARCHAR(255),
    brandcode VARCHAR(255),
    competitorrewardsgroup VARCHAR(255),
    discounteditemprice DECIMAL,
    originalreceiptitemtext VARCHAR(255),
    itemnumber VARCHAR(255),
    needsfetchreviewreason VARCHAR(255),
    originalmetabritequantitypurchased INTEGER,
    pointsearned VARCHAR(255),
    targetprice DECIMAL,
    competitiveproduct BOOLEAN,
    userflaggeddescription VARCHAR(255),
    deleted BOOLEAN,
    priceaftercoupon DECIMAL,
    metabritecampaignid VARCHAR(255),
    receipt_id INTEGER REFERENCES receipts(receipts_id) NULL
);

-- Exception table for missing brands
CREATE TABLE missing_brands (
    missing_brands_id SERIAL PRIMARY KEY,
    brandcode VARCHAR(255) UNIQUE,
    occurrence_count INTEGER DEFAULT 0,
    first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Exception table for missing users
CREATE TABLE missing_users (
    missing_users_id SERIAL PRIMARY KEY,
    user_id VARCHAR(24) UNIQUE,
    occurrence_count INTEGER DEFAULT 0,
    first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for foreign keys
CREATE INDEX idx_brands_category ON brands(category_id);
CREATE INDEX idx_receipts_user ON receipts(userid);
CREATE INDEX idx_rewardsreceiptitemlist_receipt ON rewardsreceiptitemlist(receipt_id);
CREATE INDEX idx_rewardsreceiptitem
```

## SQL Queries

### Top 5 brands by month
```sql
with monthly_stats as (
    select 
        date_trunc('month', r.datescanned) as scan_month,
        coalesce(b.name, ri.brandcode) as brand_name,
        count(distinct r.receipts_id) as receipt_count,
        sum(ri.finalprice * ri.quantitypurchased) as total_spend,
        case 
            when b.brandcode is null then 'missing from brands table'
            else 'valid brand'
        end as brand_status
    from receipts r
    join rewardsreceiptitemlist ri on r.receipts_id = ri.receipt_id
    left join brands b on ri.brandcode = b.brandcode
    left join missing_brands mb on ri.brandcode = mb.brandcode
    where ri.brandcode is not null
    group by 
        date_trunc('month', r.datescanned),
        b.name, 
        ri.brandcode, 
        b.brandcode
),
ranked_brands as (
    select 
        scan_month,
        brand_name,
        receipt_count,
        to_char(total_spend, 'FM$999,999,999.00') as total_spend_formatted,
        to_char(round(total_spend / receipt_count, 2), 'FM$999,999,999.00') as avg_spend_per_receipt,
        brand_status,
        row_number() over (partition by scan_month order by receipt_count desc) as month_rank
    from monthly_stats
    where brand_name not like 'BRAND'
)
select 
    scan_month,
    brand_name,
    receipt_count,
    total_spend_formatted as total_spend,
    avg_spend_per_receipt,
    brand_status,
    month_rank
from ranked_brands
where month_rank <= 5
order by 
    scan_month desc,
    month_rank;
```

### Average spend by receipt status
```sql
select 
    rewardsreceiptstatus,
    count(*) as receipt_count,
    to_char(avg(totalspent), 'FM$999,999,999.00') as avg_spend
from receipts 
group by rewardsreceiptstatus
order by avg(totalspent) desc nulls last;
```

### Total items by receipt status
```sql
select 
    rewardsreceiptstatus,
    count(*) as receipt_count,
    sum(purchaseditemcount) as total_items_purchased
from receipts r
group by rewardsreceiptstatus
order by total_items_purchased desc nulls last;
```

### Brand with most spend among recent users
```sql
with latest_date as (
    select max(datescanned) as max_date
    from receipts
),
recent_users as (
    select _id
    from users, latest_date
    where createddate >= (latest_date.max_date - interval '6 months')
)
select 
    case 
        when b.name is not null then b.name
        else ri.brandcode
    end as brand_name,
    count(distinct r.receipts_id) as transaction_count,
    to_char(sum(ri.finalprice * ri.quantitypurchased), 'FM$999,999,999.00') as total_spend,
    sum(ri.quantitypurchased) as total_items_purchased,
    to_char(round(sum(ri.finalprice * ri.quantitypurchased) / sum(ri.quantitypurchased), 2), 'FM$999,999,999.00') as avg_price_per_item,
    case 
        when b.brandcode is null then 'missing from brands table'
        else 'valid brand'
    end as brand_status
from receipts r
join rewardsreceiptitemlist ri on r.receipts_id = ri.receipt_id
left join brands b on ri.brandcode = b.brandcode
join recent_users u on r.userid = u._id
where ri.brandcode is not null
  and (b.name is not null or ri.brandcode is not null)
group by b.name, ri.brandcode, b.brandcode
order by sum(ri.finalprice * ri.quantitypurchased) desc
limit 5;
```

## Data Quality Analysis

### Summary of data quality issues
```sql
select 
    'missing brands' as issue_type,
    count(*) as total_count,
    sum(occurrence_count) as total_occurrences,
    min(first_seen) as earliest_seen,
    max(last_seen) as latest_seen
from missing_brands
union all
select 
    'missing users' as issue_type,
    count(*) as total_count,
    sum(occurrence_count) as total_occurrences,
    min(first_seen) as earliest_seen,
    max(last_seen) as latest_seen
from missing_users;
```

### Brand reference analysis
```sql
select mb.brandcode, count(*)
from rewardsreceiptitemlist ri
left join missing_brands mb on ri.brandcode = mb.brandcode
left join brands b on ri.brandcode = b.brandcode
group by 1
order by 1;
```

## Query Results

### Top 5 Brands by Month

| scan_month | brand_name | receipt_count | total_spend | avg_spend_per_receipt | brand_status | month_rank |
|------------|------------|---------------|-------------|----------------------|--------------|------------|
| 2021-02-01 | MISSION | 2 | $4.46 | $2.23 | missing from brands table | 1 |
| 2021-02-01 | Viva | 1 | $3.92 | $3.92 | valid brand | 2 |
| 2021-01-01 | BEN AND JERRYS | 32 | $7,264.59 | $227.02 | missing from brands table | 1 |
| 2021-01-01 | FOLGERS | 23 | $599.29 | $26.06 | missing from brands table | 2 |
| 2021-01-01 | Pepsi | 23 | $848.94 | $36.91 | valid brand | 3 |
| 2021-01-01 | KELLOGG'S | 22 | $117.07 | $5.32 | missing from brands table | 4 |
| 2021-01-01 | Kraft | 22 | $133.53 | $6.07 | valid brand | 5 |

Key observations:
- BEN AND JERRYS had the highest spend per receipt ($227.02) in January 2021
- Several major brands (BEN AND JERRYS, FOLGERS, KELLOGG'S) are missing from the brands table
- Tied receipt counts (e.g., FOLGERS and Pepsi with 23, KELLOGG'S and Kraft with 22) are ranked sequentially
- February 2021 shows significantly lower activity with only 2 brands meeting the criteria

### Average Spend by Receipt Status
| rewardsreceiptstatus | receipt_count | avg_spend |
|---------------------|---------------|------------|
| FLAGGED | 46 | $180.45 |
| FINISHED | 518 | $80.85 |
| PENDING | 50 | $28.03 |
| REJECTED | 71 | $23.33 |
| SUBMITTED | 434 | NULL |

Key observations:
- FLAGGED receipts have the highest average spend at $180.45
- SUBMITTED receipts have NULL spend values as they haven't been processed yet
- REJECTED receipts have the lowest average spend at $23.33

### Total Items by Receipt Status
| rewardsreceiptstatus | receipt_count | total_items_purchased |
|---------------------|---------------|---------------------|
| FINISHED | 518 | 8,184 |
| FLAGGED | 46 | 1,014 |
| REJECTED | 71 | 173 |
| SUBMITTED | 434 | NULL |
| PENDING | 50 | NULL |

Key observations:
- FINISHED receipts account for most items purchased
- SUBMITTED and PENDING receipts have NULL item counts as they haven't been processed
- REJECTED receipts have relatively few items per receipt

### User Activity Analysis
| activity_level | user_status | user_count | percentage |
|---------------|-------------|------------|------------|
| No receipts | Valid User | 45 | 35.71 |
| 1 receipt | Valid User | 28 | 22.22 |
| 2-5 receipts | Valid User | 38 | 30.16 |
| 6-10 receipts | Valid User | 9 | 7.14 |
| More than 10 receipts | Valid User | 6 | 4.77 |

Key observations:
- Most users (35.71%) have not submitted any receipts
- Only a small percentage (4.77%) of users are highly active with more than 10 receipts
- The majority of active users (52.38%) have between 1-5 receipts

## Data Quality Findings

### Summary of Data Quality Issues
```sql
select 
    'missing brands' as issue_type,
    count(*) as total_count,
    sum(occurrence_count) as total_occurrences,
    min(first_seen) as earliest_seen,
    max(last_seen) as latest_seen
from missing_brands
union all
select 
    'missing users' as issue_type,
    count(*) as total_count,
    sum(occurrence_count) as total_occurrences,
    min(first_seen) as earliest_seen,
    max(last_seen) as latest_seen
from missing_users;
```

| issue_type | total_count | total_occurrences | earliest_seen | latest_seen |
|------------|-------------|-------------------|---------------|-------------|
| missing brands | 187 | 1972 | 2025-01-24 01:29:17 | 2025-01-24 01:29:19 |
| missing users | 117 | 148 | 2025-01-24 01:29:17 | 2025-01-24 01:29:17 |

Key findings:
- 187 unique brands are missing from the brands table, with 1,972 total occurrences
- 117 users referenced in receipts are missing from the users table, with 148 total occurrences
- All data quality issues were detected within a 2-second window

### Receipt Status Distribution
```sql
select 
    rewardsreceiptstatus,
    count(*) as receipt_count,
    round(count(*)::numeric / sum(count(*)) over () * 100, 2) as percentage
from receipts
group by rewardsreceiptstatus
order by receipt_count desc;
```

| rewardsreceiptstatus | receipt_count | percentage |
|---------------------|---------------|------------|
| FINISHED | 518 | 46.25 |
| SUBMITTED | 434 | 38.75 |
| REJECTED | 71 | 6.34 |
| PENDING | 50 | 4.46 |
| FLAGGED | 46 | 4.11 |

Key findings:
- Nearly half (46.25%) of all receipts are in FINISHED status
- A significant portion (38.75%) remain in SUBMITTED status
- Relatively low rejection rate at 6.34%
- Small percentage of receipts require additional review (FLAGGED: 4.11%)

### Missing Brand Codes Analysis
```sql
select 
    case when brandcode is null then 'NULL'
         else brandcode 
    end as brandcode_status,
    count(*) as item_count,
    round(count(*)::numeric / sum(count(*)) over () * 100, 2) as percentage
from rewardsreceiptitemlist
group by brandcode_status
order by item_count desc;
```

Key findings:
- Significant number of items missing brand codes
- Data quality issue affects brand analysis accuracy
- Opportunity to improve brand code capture during receipt processing

### Overall Data Quality Recommendations:
1. Implement validation for brand references before receipt processing
2. Review user registration process to prevent missing user records
3. Investigate high number of SUBMITTED status receipts
4. Consider automated brand code matching system
5. Add data quality monitoring for real-time issue detection
