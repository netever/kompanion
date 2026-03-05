-- 1. ПРОЦЕДУРА ЗАГРУЗКИ HUB_CUSTOMER
CREATE OR REPLACE PROCEDURE dv.load_hub_customer()
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO dv.hub_customer (
        customer_hk,
        customerid,
        load_date,
        record_source
    )
    SELECT DISTINCT
        dv.hash_to_uuid(dv.generate_hash_key(ARRAY[customerid::TEXT])) AS customer_hk,
        customerid,
        CURRENT_TIMESTAMP AS load_date,
        'stg_onlinebankdb.dbo_customers' AS record_source
    FROM stg_onlinebankdb.dbo_customers
    WHERE customerid IS NOT NULL
    ON CONFLICT (customer_hk) DO NOTHING;
    
    RAISE NOTICE 'Hub Customer загружен. Добавлено записей: %', 
        (SELECT COUNT(*) FROM dv.hub_customer);
END;
$$;

COMMENT ON PROCEDURE dv.load_hub_customer() IS 'Загрузка хаба клиентов из staging';

-- 2. ПРОЦЕДУРА ЗАГРУЗКИ HUB_ACCOUNT
CREATE OR REPLACE PROCEDURE dv.load_hub_account()
LANGUAGE plpgsql
AS $$
BEGIN
    -- Загрузка счетов из таблицы accounts
    INSERT INTO dv.hub_account (
        account_hk,
        accountno,
        currencyid,
        load_date,
        record_source
    )
    SELECT DISTINCT
        dv.hash_to_uuid(dv.generate_hash_key(ARRAY[accountno, currencyid::TEXT])) AS account_hk,
        accountno,
        currencyid,
        CURRENT_TIMESTAMP AS load_date,
        'stg_onlinebankdb.dbo_accounts' AS record_source
    FROM stg_onlinebankdb.dbo_accounts
    WHERE accountno IS NOT NULL AND currencyid IS NOT NULL
    ON CONFLICT (account_hk) DO NOTHING;
    
    -- Загрузка дебетовых счетов из транзакций (которых может не быть в accounts)
    INSERT INTO dv.hub_account (
        account_hk,
        accountno,
        currencyid,
        load_date,
        record_source
    )
    SELECT DISTINCT
        dv.hash_to_uuid(dv.generate_hash_key(ARRAY[debetaccountno, currencyid::TEXT])) AS account_hk,
        debetaccountno AS accountno,
        currencyid,
        CURRENT_TIMESTAMP AS load_date,
        'stg_onlinebankdb.dbo_transactions' AS record_source
    FROM stg_onlinebankdb.dbo_transactions
    WHERE debetaccountno IS NOT NULL AND currencyid IS NOT NULL
    ON CONFLICT (account_hk) DO NOTHING;
    
    -- Загрузка кредитовых счетов из транзакций (которых может не быть в accounts)
    INSERT INTO dv.hub_account (
        account_hk,
        accountno,
        currencyid,
        load_date,
        record_source
    )
    SELECT DISTINCT
        dv.hash_to_uuid(dv.generate_hash_key(ARRAY[creditaccountno, currencyid::TEXT])) AS account_hk,
        creditaccountno AS accountno,
        currencyid,
        CURRENT_TIMESTAMP AS load_date,
        'stg_onlinebankdb.dbo_transactions' AS record_source
    FROM stg_onlinebankdb.dbo_transactions
    WHERE creditaccountno IS NOT NULL AND currencyid IS NOT NULL
    ON CONFLICT (account_hk) DO NOTHING;
    
    RAISE NOTICE 'Hub Account загружен. Всего записей: %', 
        (SELECT COUNT(*) FROM dv.hub_account);
END;
$$;

COMMENT ON PROCEDURE dv.load_hub_account() IS 'Загрузка хаба счетов из staging (dbo_accounts и dbo_transactions)';

-- 3. ПРОЦЕДУРА ЗАГРУЗКИ HUB_TRANSACTION
CREATE OR REPLACE PROCEDURE dv.load_hub_transaction()
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO dv.hub_transaction (
        transaction_hk,
        position,
        positionn,
        load_date,
        record_source
    )
    SELECT DISTINCT
        dv.hash_to_uuid(dv.generate_hash_key(ARRAY[position::TEXT, positionn::TEXT])) AS transaction_hk,
        position,
        positionn,
        CURRENT_TIMESTAMP AS load_date,
        'stg_onlinebankdb.dbo_transactions' AS record_source
    FROM stg_onlinebankdb.dbo_transactions
    WHERE position IS NOT NULL AND positionn IS NOT NULL
    ON CONFLICT (transaction_hk) DO NOTHING;
    
    RAISE NOTICE 'Hub Transaction загружен. Добавлено записей: %', 
        (SELECT COUNT(*) FROM dv.hub_transaction);
END;
$$;

COMMENT ON PROCEDURE dv.load_hub_transaction() IS 'Загрузка хаба транзакций из staging';

-- 4. ПРОЦЕДУРА ЗАГРУЗКИ SAT_CUSTOMER
CREATE OR REPLACE PROCEDURE dv.load_sat_customer()
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO dv.sat_customer (
        customer_hk,
        load_date,
        hash_diff,
        customername,
        record_source
    )
    SELECT
        dv.hash_to_uuid(dv.generate_hash_key(ARRAY[src.customerid::TEXT])) AS customer_hk,
        CURRENT_TIMESTAMP AS load_date,
        dv.generate_hash_diff(ARRAY[COALESCE(src.customername, '')]) AS hash_diff,
        src.customername,
        'stg_onlinebankdb.dbo_customers' AS record_source
    FROM stg_onlinebankdb.dbo_customers src
    WHERE src.customerid IS NOT NULL
    AND NOT EXISTS (
        SELECT 1
        FROM dv.sat_customer sat
        WHERE sat.customer_hk = dv.hash_to_uuid(dv.generate_hash_key(ARRAY[src.customerid::TEXT]))
        AND sat.hash_diff = dv.generate_hash_diff(ARRAY[COALESCE(src.customername, '')])
    );
    
    RAISE NOTICE 'Sat Customer загружен. Добавлено записей: %', 
        (SELECT COUNT(*) FROM dv.sat_customer);
END;
$$;

COMMENT ON PROCEDURE dv.load_sat_customer() IS 'Загрузка сателлита клиентов из staging';

-- 5. ПРОЦЕДУРА ЗАГРУЗКИ SAT_ACCOUNT
CREATE OR REPLACE PROCEDURE dv.load_sat_account()
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO dv.sat_account (
        account_hk,
        load_date,
        hash_diff,
        accountname,
        opendate,
        closedate,
        record_source
    )
    SELECT
        dv.hash_to_uuid(dv.generate_hash_key(ARRAY[src.accountno, src.currencyid::TEXT])) AS account_hk,
        CURRENT_TIMESTAMP AS load_date,
        dv.generate_hash_diff(ARRAY[
            COALESCE(src.accountname, ''),
            COALESCE(src.opendate::TEXT, ''),
            COALESCE(src.closedate::TEXT, '')
        ]) AS hash_diff,
        src.accountname,
        src.opendate,
        src.closedate,
        'stg_onlinebankdb.dbo_accounts' AS record_source
    FROM stg_onlinebankdb.dbo_accounts src
    WHERE src.accountno IS NOT NULL AND src.currencyid IS NOT NULL
    AND NOT EXISTS (
        SELECT 1
        FROM dv.sat_account sat
        WHERE sat.account_hk = dv.hash_to_uuid(dv.generate_hash_key(ARRAY[src.accountno, src.currencyid::TEXT]))
        AND sat.hash_diff = dv.generate_hash_diff(ARRAY[
            COALESCE(src.accountname, ''),
            COALESCE(src.opendate::TEXT, ''),
            COALESCE(src.closedate::TEXT, '')
        ])
    );
    
    RAISE NOTICE 'Sat Account загружен. Добавлено записей: %', 
        (SELECT COUNT(*) FROM dv.sat_account);
END;
$$;

COMMENT ON PROCEDURE dv.load_sat_account() IS 'Загрузка сателлита счетов из staging';

-- 6. ПРОЦЕДУРА ЗАГРУЗКИ SAT_TRANSACTION
CREATE OR REPLACE PROCEDURE dv.load_sat_transaction()
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO dv.sat_transaction (
        transaction_hk,
        load_date,
        hash_diff,
        currencyid,
        transactiondate,
        sumn,
        record_source
    )
    SELECT
        dv.hash_to_uuid(dv.generate_hash_key(ARRAY[src.position::TEXT, src.positionn::TEXT])) AS transaction_hk,
        CURRENT_TIMESTAMP AS load_date,
        dv.generate_hash_diff(ARRAY[
            COALESCE(src.currencyid::TEXT, ''),
            COALESCE(src.transactiondate::TEXT, ''),
            COALESCE(src.sumn::TEXT, '')
        ]) AS hash_diff,
        src.currencyid,
        src.transactiondate,
        src.sumn,
        'stg_onlinebankdb.dbo_transactions' AS record_source
    FROM stg_onlinebankdb.dbo_transactions src
    WHERE src.position IS NOT NULL AND src.positionn IS NOT NULL
    AND NOT EXISTS (
        SELECT 1
        FROM dv.sat_transaction sat
        WHERE sat.transaction_hk = dv.hash_to_uuid(dv.generate_hash_key(ARRAY[src.position::TEXT, src.positionn::TEXT]))
        AND sat.hash_diff = dv.generate_hash_diff(ARRAY[
            COALESCE(src.currencyid::TEXT, ''),
            COALESCE(src.transactiondate::TEXT, ''),
            COALESCE(src.sumn::TEXT, '')
        ])
    );
    
    RAISE NOTICE 'Sat Transaction загружен. Добавлено записей: %', 
        (SELECT COUNT(*) FROM dv.sat_transaction);
END;
$$;

COMMENT ON PROCEDURE dv.load_sat_transaction() IS 'Загрузка сателлита транзакций из staging';

-- 7. ПРОЦЕДУРА ЗАГРУЗКИ LINK_ACCOUNT_CUSTOMER
CREATE OR REPLACE PROCEDURE dv.load_link_account_customer()
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO dv.link_account_customer (
        link_account_customer_hk,
        account_hk,
        customer_hk,
        load_date,
        record_source
    )
    SELECT DISTINCT
        dv.hash_to_uuid(dv.generate_hash_key(ARRAY[
            src.accountno,
            src.currencyid::TEXT,
            src.customerid::TEXT
        ])) AS link_account_customer_hk,
        dv.hash_to_uuid(dv.generate_hash_key(ARRAY[src.accountno, src.currencyid::TEXT])) AS account_hk,
        dv.hash_to_uuid(dv.generate_hash_key(ARRAY[src.customerid::TEXT])) AS customer_hk,
        CURRENT_TIMESTAMP AS load_date,
        'stg_onlinebankdb.dbo_accounts' AS record_source
    FROM stg_onlinebankdb.dbo_accounts src
    WHERE src.accountno IS NOT NULL 
        AND src.currencyid IS NOT NULL 
        AND src.customerid IS NOT NULL
    ON CONFLICT (link_account_customer_hk) DO NOTHING;
    
    RAISE NOTICE 'Link Account-Customer загружен. Добавлено записей: %', 
        (SELECT COUNT(*) FROM dv.link_account_customer);
END;
$$;

COMMENT ON PROCEDURE dv.load_link_account_customer() IS 'Загрузка связи счетов и клиентов из staging';

-- 8. ПРОЦЕДУРА ЗАГРУЗКИ LINK_TRANSACTION_ACCOUNT
CREATE OR REPLACE PROCEDURE dv.load_link_transaction_account()
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO dv.link_transaction_account (
        link_transaction_account_hk,
        transaction_hk,
        debet_account_hk,
        credit_account_hk,
        load_date,
        record_source
    )
    SELECT DISTINCT
        dv.hash_to_uuid(dv.generate_hash_key(ARRAY[
            src.position::TEXT,
            src.positionn::TEXT,
            src.debetaccountno,
            src.creditaccountno,
            src.currencyid::TEXT
        ])) AS link_transaction_account_hk,
        dv.hash_to_uuid(dv.generate_hash_key(ARRAY[src.position::TEXT, src.positionn::TEXT])) AS transaction_hk,
        dv.hash_to_uuid(dv.generate_hash_key(ARRAY[src.debetaccountno, src.currencyid::TEXT])) AS debet_account_hk,
        dv.hash_to_uuid(dv.generate_hash_key(ARRAY[src.creditaccountno, src.currencyid::TEXT])) AS credit_account_hk,
        CURRENT_TIMESTAMP AS load_date,
        'stg_onlinebankdb.dbo_transactions' AS record_source
    FROM stg_onlinebankdb.dbo_transactions src
    WHERE src.position IS NOT NULL 
        AND src.positionn IS NOT NULL
        AND src.debetaccountno IS NOT NULL
        AND src.creditaccountno IS NOT NULL
        AND src.currencyid IS NOT NULL
    ON CONFLICT (link_transaction_account_hk) DO NOTHING;
    
    RAISE NOTICE 'Link Transaction-Account загружен. Добавлено записей: %', 
        (SELECT COUNT(*) FROM dv.link_transaction_account);
END;
$$;

COMMENT ON PROCEDURE dv.load_link_transaction_account() IS 'Загрузка связи транзакций и счетов из staging';

-- 9. МАСТЕР-ПРОЦЕДУРА ПОЛНОЙ ЗАГРУЗКИ
CREATE OR REPLACE PROCEDURE dv.load_all_data_vault()
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE 'НАЧАЛО ЗАГРУЗКИ DATA VAULT';
    RAISE NOTICE '========================================';
    
    -- Шаг 1: Загрузка всех Hubs
    RAISE NOTICE 'Шаг 1: Загрузка Hubs...';
    CALL dv.load_hub_customer();
    CALL dv.load_hub_account();
    CALL dv.load_hub_transaction();
    
    -- Шаг 2: Загрузка всех Satellites
    RAISE NOTICE 'Шаг 2: Загрузка Satellites...';
    CALL dv.load_sat_customer();
    CALL dv.load_sat_account();
    CALL dv.load_sat_transaction();
    
    -- Шаг 3: Загрузка всех Links
    RAISE NOTICE 'Шаг 3: Загрузка Links...';
    CALL dv.load_link_account_customer();
    CALL dv.load_link_transaction_account();
    
    RAISE NOTICE '========================================';
    RAISE NOTICE 'ЗАГРУЗКА DATA VAULT ЗАВЕРШЕНА УСПЕШНО!';
    RAISE NOTICE '========================================';
END;
$$;

COMMENT ON PROCEDURE dv.load_all_data_vault() IS 'Мастер-процедура полной загрузки всех объектов Data Vault';

-- ВСПОМОГАТЕЛЬНЫЕ СКРИПТЫ ДЛЯ ПРОВЕРКИ

-- Запрос для проверки количества записей во всех таблицах
CREATE OR REPLACE VIEW dv.v_data_vault_summary AS
SELECT 
    'hub_customer' AS table_name,
    COUNT(*) AS record_count
FROM dv.hub_customer
UNION ALL
SELECT 'hub_account', COUNT(*) FROM dv.hub_account
UNION ALL
SELECT 'hub_transaction', COUNT(*) FROM dv.hub_transaction
UNION ALL
SELECT 'sat_customer', COUNT(*) FROM dv.sat_customer
UNION ALL
SELECT 'sat_account', COUNT(*) FROM dv.sat_account
UNION ALL
SELECT 'sat_transaction', COUNT(*) FROM dv.sat_transaction
UNION ALL
SELECT 'link_account_customer', COUNT(*) FROM dv.link_account_customer
UNION ALL
SELECT 'link_transaction_account', COUNT(*) FROM dv.link_transaction_account;

COMMENT ON VIEW dv.v_data_vault_summary IS 'Сводка по количеству записей в Data Vault';

-- ПРИМЕР ИСПОЛЬЗОВАНИЯ

-- Запуск полной загрузки:
-- CALL dv.load_all_data_vault();

-- Проверка результатов:
-- SELECT * FROM dv.v_data_vault_summary;

-- Или запуск отдельных процедур:
-- CALL dv.load_hub_customer();
-- CALL dv.load_sat_customer();
-- CALL dv.load_link_account_customer();
