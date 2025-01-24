-- Monthly brand performance - top 5 brands per month
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
    where  brand_name not like 'BRAND'
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

-- when considering average spend from receipts with 'rewardsreceiptstatus' of 'accepted' or 'rejected', which is greater?
select 
    rewardsreceiptstatus,
    count(*) as receipt_count,
    to_char(avg(totalspent), 'FM$999,999,999.00') as avg_spend
from receipts 
--where lower(rewardsreceiptstatus) in ('accepted', 'rejected')
group by rewardsreceiptstatus
order by avg(totalspent) desc nulls last;

-- when considering total number of items purchased from receipts with 'rewardsreceiptstatus' of 'accepted' or 'rejected', which is greater?
select 
    rewardsreceiptstatus,
    count(*) as receipt_count,
    sum(purchaseditemcount) as total_items_purchased
from receipts r
--where lower(rewardsreceiptstatus) in ('accepted', 'rejected')
group by rewardsreceiptstatus
order by total_items_purchased desc nulls last;

-- which brand has the most spend among users who were created within the past 6 months?
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

-- which brand has the most transactions among users who were created within the past 6 months?
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
  and  b.name not like 'BRAND'
group by b.name, ri.brandcode, b.brandcode
order by transaction_count desc
limit 5;

-- summary of data quality issues
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

-- Brand reference analysis
select mb.brandcode, count(*)
from rewardsreceiptitemlist ri
left join missing_brands mb on ri.brandcode = mb.brandcode
left join brands b on ri.brandcode = b.brandcode
group by 1
order by 1;

-- Receipt status distribution
select 
    rewardsreceiptstatus,
    count(*) as receipt_count,
    round(count(*)::numeric / sum(count(*)) over () * 100, 2) as percentage
from receipts
group by rewardsreceiptstatus
order by receipt_count desc;

-- Missing brand codes in receipt items
select 
    case when brandcode is null then 'NULL'
         else brandcode 
    end as brandcode_status,
    count(*) as item_count,
    round(count(*)::numeric / sum(count(*)) over () * 100, 2) as percentage
from rewardsreceiptitemlist
group by brandcode_status
order by item_count desc;

-- User activity analysis
WITH all_users AS (
    -- Get all users from both tables
    SELECT _id as user_id FROM users
    UNION
    SELECT userid FROM receipts WHERE userid NOT IN (SELECT _id FROM users)
),
user_receipts AS (
    -- Count receipts for all users (including missing ones)
    SELECT 
        au.user_id,
        count(r.receipts_id) as receipt_count,
        CASE WHEN u._id IS NULL THEN 'Missing User' ELSE 'Valid User' END as user_status
    FROM all_users au
    LEFT JOIN users u ON au.user_id = u._id
    LEFT JOIN receipts r ON au.user_id = r.userid
    GROUP BY au.user_id, CASE WHEN u._id IS NULL THEN 'Missing User' ELSE 'Valid User' END
),
user_categories AS (
    SELECT 
        CASE 
            WHEN receipt_count = 0 THEN 'No receipts'
            WHEN receipt_count = 1 THEN '1 receipt'
            WHEN receipt_count BETWEEN 2 AND 5 THEN '2-5 receipts'
            WHEN receipt_count BETWEEN 6 AND 10 THEN '6-10 receipts'
            ELSE 'More than 10 receipts'
        END as activity_level,
        user_status,
        count(*) as user_count
    FROM user_receipts
    GROUP BY 
        CASE 
            WHEN receipt_count = 0 THEN 'No receipts'
            WHEN receipt_count = 1 THEN '1 receipt'
            WHEN receipt_count BETWEEN 2 AND 5 THEN '2-5 receipts'
            WHEN receipt_count BETWEEN 6 AND 10 THEN '6-10 receipts'
            ELSE 'More than 10 receipts'
        END,
        user_status
)
SELECT 
    activity_level,
    user_status,
    user_count,
    round(user_count::numeric / sum(user_count) over () * 100, 2) as percentage
FROM user_categories
ORDER BY 
    user_status,
    CASE activity_level
        WHEN 'No receipts' THEN 1
        WHEN '1 receipt' THEN 2
        WHEN '2-5 receipts' THEN 3
        WHEN '6-10 receipts' THEN 4
        ELSE 5
    END;





