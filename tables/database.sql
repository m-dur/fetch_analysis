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
CREATE INDEX idx_rewardsreceiptitemlist_brand ON rewardsreceiptitemlist(brandcode);


