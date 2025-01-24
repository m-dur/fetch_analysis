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
[Add your query results here]

## Data Quality Findings
[Add your data quality findings here]
