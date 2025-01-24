```mermaid
erDiagram
    CATEGORIES ||--o{ BRANDS : "categorizes"
    USERS ||--o{ RECEIPTS : submits
    BRANDS |o--o{ REWARDSRECEIPTITEMLIST : "may appear in"
    RECEIPTS ||--o{ REWARDSRECEIPTITEMLIST : contains
    MISSING_BRANDS ||--o{ REWARDSRECEIPTITEMLIST : "tracks missing from"
    MISSING_USERS ||--o{ RECEIPTS : "tracks missing from"

    CATEGORIES {
        SERIAL categories_id PK
        VARCHAR(255) category UK "UNIQUE"
        VARCHAR(255) categorycode
    }

    BRANDS {
        SERIAL brands_id PK
        VARCHAR(24) _id
        VARCHAR(255) barcode
        VARCHAR(255) name
        BOOLEAN topbrand
        VARCHAR(255) brandcode UK "UNIQUE"
        INTEGER category_id FK
    }

    USERS {
        SERIAL users_id PK
        VARCHAR(24) _id UK "UNIQUE"
        BOOLEAN active
        TIMESTAMP createddate
        TIMESTAMP lastlogin
        VARCHAR(255) role
        VARCHAR(255) signupsource
        VARCHAR(255) state
    }

    RECEIPTS {
        SERIAL receipts_id PK
        VARCHAR(24) _id
        INTEGER bonuspointsearned
        VARCHAR(255) bonuspointsearnedreason
        TIMESTAMP createdate
        TIMESTAMP datescanned
        TIMESTAMP finisheddate
        TIMESTAMP modifydate
        TIMESTAMP pointsawardeddate
        VARCHAR(255) pointsearned
        TIMESTAMP purchasedate
        INTEGER purchaseditemcount
        VARCHAR(255) rewardsreceiptstatus
        DECIMAL totalspent
        VARCHAR(24) userid FK "REFERENCES users(_id)"
    }

    REWARDSRECEIPTITEMLIST {
        SERIAL rewardsreceiptitemlist_id PK
        VARCHAR(255) barcode
        VARCHAR(255) description
        DECIMAL finalprice
        DECIMAL itemprice
        BOOLEAN needsfetchreview
        VARCHAR(255) partneritemid
        BOOLEAN preventtargetgappoints
        INTEGER quantitypurchased
        VARCHAR(255) userflaggedbarcode
        BOOLEAN userflaggednewitem
        DECIMAL userflaggedprice
        INTEGER userflaggedquantity
        VARCHAR(255) originalmetabritebarcode
        VARCHAR(255) originalmetabritedescription
        VARCHAR(255) pointsnotawardedreason
        VARCHAR(255) pointspayerid
        VARCHAR(255) rewardsgroup
        VARCHAR(255) rewardsproductpartnerid
        VARCHAR(255) brandcode
        VARCHAR(255) competitorrewardsgroup
        DECIMAL discounteditemprice
        VARCHAR(255) originalreceiptitemtext
        VARCHAR(255) itemnumber
        VARCHAR(255) needsfetchreviewreason
        INTEGER originalmetabritequantitypurchased
        VARCHAR(255) pointsearned
        DECIMAL targetprice
        BOOLEAN competitiveproduct
        VARCHAR(255) userflaggeddescription
        BOOLEAN deleted
        DECIMAL priceaftercoupon
        VARCHAR(255) metabritecampaignid
        INTEGER receipt_id FK "REFERENCES receipts(receipts_id)"
    }

    MISSING_BRANDS {
        SERIAL missing_brands_id PK
        VARCHAR(255) brandcode UK "UNIQUE"
        INTEGER occurrence_count
        TIMESTAMP first_seen
        TIMESTAMP last_seen
    }

    MISSING_USERS {
        SERIAL missing_users_id PK
        VARCHAR(24) user_id UK "UNIQUE"
        INTEGER occurrence_count
        TIMESTAMP first_seen
        TIMESTAMP last_seen
    }