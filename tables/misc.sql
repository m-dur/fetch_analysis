-- testing - showing all tables in my database
SELECT table_name 
FROM information_schema.tables 
WHERE table_schema = 'public' 
ORDER BY table_name;

-- testing -- showing row counts of all tables
ANALYZE VERBOSE;
SELECT 
    schemaname as schema_name,
    relname as table_name,
    n_live_tup as row_count
FROM pg_stat_user_tables
ORDER BY n_live_tup DESC;

-- testing -- deleting all rows in all tables
DO $$ 
DECLARE 
    row record;
BEGIN
    FOR row IN (SELECT tablename FROM pg_tables WHERE schemaname = 'public') 
    LOOP
        EXECUTE 'TRUNCATE TABLE ' || quote_ident(row.tablename) || ' CASCADE';
    END LOOP;
END $$;

-- testing -- dropping all tables in database
DO $$ 
DECLARE 
    row record;
BEGIN
    FOR row IN (
        SELECT tablename FROM pg_tables 
        WHERE schemaname = 'public'
    ) LOOP
        EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(row.tablename) || ' CASCADE';
    END LOOP;
END $$;



