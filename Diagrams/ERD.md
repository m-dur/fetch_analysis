```mermaid
erDiagram
    CATEGORIES ||--o{ BRANDS : "categorizes"
    USERS ||--o{ RECEIPTS : submits
    BRANDS |o--o{ REWARDSRECEIPTITEMLIST : "may appear in"
    RECEIPTS ||--o{ REWARDSRECEIPTITEMLIST : contains
    MISSING_BRANDS ||--o{ REWARDSRECEIPTITEMLIST : "tracks missing from"
    MISSING_USERS ||--o{ RECEIPTS : "tracks missing from"

    CATEGORIES {
        SERIAL categories_id PK "Primary key for categories"
        VARCHAR(255) category UK "Unique category name"
        VARCHAR(255) categorycode "Internal code for category"
    }

    BRANDS {
        SERIAL brands_id PK "Primary key for brands"
        VARCHAR(24) _id "Original brand ID"
        VARCHAR(255) barcode "Product barcode"
        VARCHAR(255) name "Brand name"
        BOOLEAN topbrand "Flag for premium brands"
        VARCHAR(255) brandcode UK "Unique brand identifier"
        INTEGER category_id FK "References categories table"
    }

    USERS {
        SERIAL users_id PK "Primary key for users"
        VARCHAR(24) _id UK "Unique user identifier"
        BOOLEAN active "Whether user is active"
        TIMESTAMP createddate "When user account was created"
        TIMESTAMP lastlogin "User's most recent login"
        VARCHAR(255) role "User's role in system"
        VARCHAR(255) signupsource "How user registered"
        VARCHAR(255) state "User's geographical state"
    }

    RECEIPTS {
        SERIAL receipts_id PK "Primary key for receipts"
        VARCHAR(24) _id "Original receipt ID"
        INTEGER bonuspointsearned "Extra points awarded"
        VARCHAR(255) bonuspointsearnedreason "Why bonus points were given"
        TIMESTAMP createdate "When receipt was created"
        TIMESTAMP datescanned "When receipt was scanned"
        TIMESTAMP finisheddate "When receipt processing completed"
        TIMESTAMP modifydate "Last modification date"
        TIMESTAMP pointsawardeddate "When points were credited"
        VARCHAR(255) pointsearned "Points earned from receipt"
        TIMESTAMP purchasedate "When purchase was made"
        INTEGER purchaseditemcount "Number of items bought"
        VARCHAR(255) rewardsreceiptstatus "Current receipt status"
        DECIMAL totalspent "Total purchase amount"
        VARCHAR(24) userid FK "References users table"
    }

    REWARDSRECEIPTITEMLIST {
        SERIAL rewardsreceiptitemlist_id PK "Primary key for receipt items"
        VARCHAR(255) barcode "Product barcode"
        VARCHAR(255) description "Item description"
        DECIMAL finalprice "Final price paid"
        DECIMAL itemprice "Original item price"
        BOOLEAN needsfetchreview "Requires review flag"
        VARCHAR(255) partneritemid "Partner's item identifier"
        BOOLEAN preventtargetgappoints "Points restriction flag"
        INTEGER quantitypurchased "Quantity bought"
        VARCHAR(255) brandcode "Brand identifier"
        DECIMAL targetprice "Target price point"
        BOOLEAN competitiveproduct "Competitor item flag"
        DECIMAL priceaftercoupon "Price after discounts"
        INTEGER receipt_id FK "References receipts table"
    }

    MISSING_BRANDS {
        SERIAL missing_brands_id PK "Primary key for missing brands"
        VARCHAR(255) brandcode UK "Unique brand identifier"
        INTEGER occurrence_count "Times brand was missing"
        TIMESTAMP first_seen "First occurrence date"
        TIMESTAMP last_seen "Most recent occurrence"
    }

    MISSING_USERS {
        SERIAL missing_users_id PK "Primary key for missing users"
        VARCHAR(24) user_id UK "Unique user identifier"
        INTEGER occurrence_count "Times user was missing"
        TIMESTAMP first_seen "First occurrence date"
        TIMESTAMP last_seen "Most recent occurrence"
    }