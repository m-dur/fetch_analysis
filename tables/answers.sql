-- what are the top 5 brands by receipts scanned for most recent month?
with latest_month as (
    select date_trunc('month', max(datescanned)) as max_month
    from receipts
)
select 
    coalesce(b.name, 'missing_brand_' || ri.brandcode) as brand_name,
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
where date_trunc('month', r.datescanned) = (select max_month from latest_month)
group by b.name, ri.brandcode, b.brandcode
order by receipt_count desc

-- how does the ranking of the top 5 brands by receipts scanned for the recent month compare to the previous month?
with month_ranks as (
    select 
        coalesce(b.name, 'missing_brand_' || ri.brandcode) as brand_name,
        date_trunc('month', r.datescanned) as scan_month,
        count(distinct r.receipts_id) as receipt_count,
        sum(ri.finalprice * ri.quantitypurchased) as total_spend,
        rank() over (partition by date_trunc('month', r.datescanned) 
                     order by count(distinct r.receipts_id) desc) as rank,
        case 
            when b.brandcode is null then 'missing from brands table'
            else 'valid brand'
        end as brand_status
    from receipts r
    join rewardsreceiptitemlist ri on r.receipts_id = ri.receipt_id
    left join brands b on ri.brandcode = b.brandcode
    left join missing_brands mb on ri.brandcode = mb.brandcode
    where date_trunc('month', r.datescanned) in (
        select distinct date_trunc('month', datescanned)
        from receipts
        order by date_trunc('month', datescanned) desc
        limit 2
    )
    group by b.name, ri.brandcode, b.brandcode, date_trunc('month', r.datescanned)
)
select 
    brand_name,
    scan_month,
    receipt_count,
    total_spend,
    rank,
    brand_status
from month_ranks
order by scan_month desc, rank;

-- when considering average spend from receipts with 'rewardsreceiptstatus' of 'accepted' or 'rejected', which is greater?
select 
    rewardsreceiptstatus,
    count(*) as receipt_count,
    avg(totalspent) as avg_spend,
    count(distinct case when u._id is null then mu.user_id end) as missing_users_count
from receipts r
left join users u on r.userid = u._id
left join missing_users mu on r.userid = mu.user_id
--where lower(rewardsreceiptstatus) in ('accepted', 'rejected')
group by rewardsreceiptstatus
order by avg_spend desc;

-- when considering total number of items purchased from receipts with 'rewardsreceiptstatus' of 'accepted' or 'rejected', which is greater?
select 
    rewardsreceiptstatus,
    count(*) as receipt_count,
    sum(purchaseditemcount) as total_items_purchased,
    count(distinct case when u._id is null then mu.user_id end) as missing_users_count
from receipts r
left join users u on r.userid = u._id
left join missing_users mu on r.userid = mu.user_id
--where lower(rewardsreceiptstatus) in ('accepted', 'rejected')
group by rewardsreceiptstatus
order by total_items_purchased desc;

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
        else 'missing_brand_' || ri.brandcode
    end as brand_name,
    count(distinct r.receipts_id) as transaction_count,
    sum(ri.finalprice * ri.quantitypurchased) as total_spend,
    sum(ri.quantitypurchased) as total_items_purchased,
    round(sum(ri.finalprice * ri.quantitypurchased) / sum(ri.quantitypurchased), 2) as avg_price_per_item,
    case 
        when b.brandcode is null then 'missing from brands table'
        else 'valid brand'
    end as brand_status
from receipts r
join rewardsreceiptitemlist ri on r.receipts_id = ri.receipt_id
left join brands b on ri.brandcode = b.brandcode
join recent_users u on r.userid = u._id
group by b.name, ri.brandcode, b.brandcode
order by total_spend desc
limit 6;

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
        else 'missing_brand_' || ri.brandcode
    end as brand_name,
    count(distinct r.receipts_id) as transaction_count,
    sum(ri.finalprice * ri.quantitypurchased) as total_spend,
    sum(ri.quantitypurchased) as total_items_purchased,
    round(sum(ri.finalprice * ri.quantitypurchased) / sum(ri.quantitypurchased), 2) as avg_price_per_item,
    case 
        when b.brandcode is null then 'missing from brands table'
        else 'valid brand'
    end as brand_status
from receipts r
join rewardsreceiptitemlist ri on r.receipts_id = ri.receipt_id
left join brands b on ri.brandcode = b.brandcode
join recent_users u on r.userid = u._id
group by b.name, ri.brandcode, b.brandcode
order by transaction_count desc
limit 6

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


select mb.brandcode, count(*)
from rewardsreceiptitemlist ri
left join missing_brands mb on ri.brandcode = mb.brandcode
left join brands b on ri.brandcode = b.brandcode
group by 1
order by 1



