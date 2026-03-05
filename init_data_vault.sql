-- Создание схемы для Data Vault
CREATE SCHEMA IF NOT EXISTS dv;

-- ========================================
-- HUBS (Бизнес-ключи)
-- ========================================

-- Hub Customer (Клиенты)
CREATE TABLE dv.hub_customer (
    customer_hk UUID NOT NULL PRIMARY KEY,
    customerid INT NOT NULL,
    load_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    record_source VARCHAR(50) NOT NULL
);

COMMENT ON TABLE dv.hub_customer IS 'Хаб клиентов';
COMMENT ON COLUMN dv.hub_customer.customer_hk IS 'Хэш-ключ клиента (PK)';
COMMENT ON COLUMN dv.hub_customer.customerid IS 'ID клиента (бизнес-ключ)';
COMMENT ON COLUMN dv.hub_customer.load_date IS 'Дата загрузки записи';
COMMENT ON COLUMN dv.hub_customer.record_source IS 'Система-источник данных';

CREATE UNIQUE INDEX idx_hub_customer_bk ON dv.hub_customer(customerid);
CREATE INDEX idx_hub_customer_load_date ON dv.hub_customer(load_date);

-- Hub Account (Лицевые счета)
CREATE TABLE dv.hub_account (
    account_hk UUID NOT NULL PRIMARY KEY,
    accountno TEXT NOT NULL,
    currencyid INT NOT NULL,
    load_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    record_source VARCHAR(50) NOT NULL
);

COMMENT ON TABLE dv.hub_account IS 'Хаб лицевых счетов';
COMMENT ON COLUMN dv.hub_account.account_hk IS 'Хэш-ключ счета (PK)';
COMMENT ON COLUMN dv.hub_account.accountno IS 'Номер счета (бизнес-ключ)';
COMMENT ON COLUMN dv.hub_account.currencyid IS 'ID валюты (бизнес-ключ)';
COMMENT ON COLUMN dv.hub_account.load_date IS 'Дата загрузки записи';
COMMENT ON COLUMN dv.hub_account.record_source IS 'Система-источник данных';

CREATE UNIQUE INDEX idx_hub_account_bk ON dv.hub_account(accountno, currencyid);
CREATE INDEX idx_hub_account_load_date ON dv.hub_account(load_date);

-- Hub Transaction (Транзакции)
CREATE TABLE dv.hub_transaction (
    transaction_hk UUID NOT NULL PRIMARY KEY,
    position BIGINT NOT NULL,
    positionn SMALLINT NOT NULL,
    load_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    record_source VARCHAR(50) NOT NULL
);

COMMENT ON TABLE dv.hub_transaction IS 'Хаб транзакций';
COMMENT ON COLUMN dv.hub_transaction.transaction_hk IS 'Хэш-ключ транзакции (PK)';
COMMENT ON COLUMN dv.hub_transaction.position IS 'Порядковый номер проводки (бизнес-ключ)';
COMMENT ON COLUMN dv.hub_transaction.positionn IS 'Порядковый номер подпроводки (бизнес-ключ)';
COMMENT ON COLUMN dv.hub_transaction.load_date IS 'Дата загрузки записи';
COMMENT ON COLUMN dv.hub_transaction.record_source IS 'Система-источник данных';

CREATE UNIQUE INDEX idx_hub_transaction_bk ON dv.hub_transaction(position, positionn);
CREATE INDEX idx_hub_transaction_load_date ON dv.hub_transaction(load_date);

-- ========================================
-- SATELLITES (Атрибуты)
-- ========================================

-- Satellite Customer (Атрибуты клиентов)
CREATE TABLE dv.sat_customer (
    customer_hk UUID NOT NULL,
    load_date TIMESTAMP NOT NULL,
    hash_diff CHAR(32) NOT NULL,
    customername TEXT NULL,
    record_source VARCHAR(50) NOT NULL,
    PRIMARY KEY (customer_hk, load_date),
    CONSTRAINT fk_sat_customer_hub 
        FOREIGN KEY (customer_hk) 
        REFERENCES dv.hub_customer(customer_hk)
);

COMMENT ON TABLE dv.sat_customer IS 'Сателлит атрибутов клиентов';
COMMENT ON COLUMN dv.sat_customer.customer_hk IS 'Хэш-ключ клиента (FK)';
COMMENT ON COLUMN dv.sat_customer.load_date IS 'Дата загрузки версии записи';
COMMENT ON COLUMN dv.sat_customer.hash_diff IS 'Хэш для отслеживания изменений атрибутов';
COMMENT ON COLUMN dv.sat_customer.customername IS 'Имя клиента';
COMMENT ON COLUMN dv.sat_customer.record_source IS 'Система-источник данных';

CREATE INDEX idx_sat_customer_hash_diff ON dv.sat_customer(hash_diff);
CREATE INDEX idx_sat_customer_load_date ON dv.sat_customer(load_date);

-- Satellite Account (Атрибуты счетов)
CREATE TABLE dv.sat_account (
    account_hk UUID NOT NULL,
    load_date TIMESTAMP NOT NULL,
    hash_diff CHAR(32) NOT NULL,
    accountname TEXT NULL,
    opendate DATE NULL,
    closedate DATE NULL,
    record_source VARCHAR(50) NOT NULL,
    PRIMARY KEY (account_hk, load_date),
    CONSTRAINT fk_sat_account_hub 
        FOREIGN KEY (account_hk) 
        REFERENCES dv.hub_account(account_hk)
);

COMMENT ON TABLE dv.sat_account IS 'Сателлит атрибутов счетов';
COMMENT ON COLUMN dv.sat_account.account_hk IS 'Хэш-ключ счета (FK)';
COMMENT ON COLUMN dv.sat_account.load_date IS 'Дата загрузки версии записи';
COMMENT ON COLUMN dv.sat_account.hash_diff IS 'Хэш для отслеживания изменений атрибутов';
COMMENT ON COLUMN dv.sat_account.accountname IS 'Наименование счета';
COMMENT ON COLUMN dv.sat_account.opendate IS 'Дата открытия счета';
COMMENT ON COLUMN dv.sat_account.closedate IS 'Дата закрытия счета';
COMMENT ON COLUMN dv.sat_account.record_source IS 'Система-источник данных';

CREATE INDEX idx_sat_account_hash_diff ON dv.sat_account(hash_diff);
CREATE INDEX idx_sat_account_load_date ON dv.sat_account(load_date);

-- Satellite Transaction (Атрибуты транзакций)
CREATE TABLE dv.sat_transaction (
    transaction_hk UUID NOT NULL,
    load_date TIMESTAMP NOT NULL,
    hash_diff CHAR(32) NOT NULL,
    currencyid INT NULL,
    transactiondate DATE NULL,
    sumn NUMERIC(15, 2) NULL,
    record_source VARCHAR(50) NOT NULL,
    PRIMARY KEY (transaction_hk, load_date),
    CONSTRAINT fk_sat_transaction_hub 
        FOREIGN KEY (transaction_hk) 
        REFERENCES dv.hub_transaction(transaction_hk)
);

COMMENT ON TABLE dv.sat_transaction IS 'Сателлит атрибутов транзакций';
COMMENT ON COLUMN dv.sat_transaction.transaction_hk IS 'Хэш-ключ транзакции (FK)';
COMMENT ON COLUMN dv.sat_transaction.load_date IS 'Дата загрузки версии записи';
COMMENT ON COLUMN dv.sat_transaction.hash_diff IS 'Хэш для отслеживания изменений атрибутов';
COMMENT ON COLUMN dv.sat_transaction.currencyid IS 'ID валюты';
COMMENT ON COLUMN dv.sat_transaction.transactiondate IS 'Дата транзакции';
COMMENT ON COLUMN dv.sat_transaction.sumn IS 'Сумма транзакции';
COMMENT ON COLUMN dv.sat_transaction.record_source IS 'Система-источник данных';

CREATE INDEX idx_sat_transaction_hash_diff ON dv.sat_transaction(hash_diff);
CREATE INDEX idx_sat_transaction_load_date ON dv.sat_transaction(load_date);
CREATE INDEX idx_sat_transaction_date ON dv.sat_transaction(transactiondate);

-- ========================================
-- LINKS (Связи между сущностями)
-- ========================================

-- Link Account-Customer (Связь счетов и клиентов)
CREATE TABLE dv.link_account_customer (
    link_account_customer_hk UUID NOT NULL PRIMARY KEY,
    account_hk UUID NOT NULL,
    customer_hk UUID NOT NULL,
    load_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    record_source VARCHAR(50) NOT NULL,
    CONSTRAINT fk_link_ac_account 
        FOREIGN KEY (account_hk) 
        REFERENCES dv.hub_account(account_hk),
    CONSTRAINT fk_link_ac_customer 
        FOREIGN KEY (customer_hk) 
        REFERENCES dv.hub_customer(customer_hk)
);

COMMENT ON TABLE dv.link_account_customer IS 'Линк связи счетов и клиентов';
COMMENT ON COLUMN dv.link_account_customer.link_account_customer_hk IS 'Хэш-ключ связи (PK)';
COMMENT ON COLUMN dv.link_account_customer.account_hk IS 'Хэш-ключ счета (FK)';
COMMENT ON COLUMN dv.link_account_customer.customer_hk IS 'Хэш-ключ клиента (FK)';
COMMENT ON COLUMN dv.link_account_customer.load_date IS 'Дата загрузки связи';
COMMENT ON COLUMN dv.link_account_customer.record_source IS 'Система-источник данных';

CREATE UNIQUE INDEX idx_link_ac_bk ON dv.link_account_customer(account_hk, customer_hk);
CREATE INDEX idx_link_ac_account ON dv.link_account_customer(account_hk);
CREATE INDEX idx_link_ac_customer ON dv.link_account_customer(customer_hk);
CREATE INDEX idx_link_ac_load_date ON dv.link_account_customer(load_date);

-- Link Transaction-Account (Связь транзакций и счетов)
CREATE TABLE dv.link_transaction_account (
    link_transaction_account_hk UUID NOT NULL PRIMARY KEY,
    transaction_hk UUID NOT NULL,
    debet_account_hk UUID NOT NULL,
    credit_account_hk UUID NOT NULL,
    load_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    record_source VARCHAR(50) NOT NULL,
    CONSTRAINT fk_link_ta_transaction 
        FOREIGN KEY (transaction_hk) 
        REFERENCES dv.hub_transaction(transaction_hk),
    CONSTRAINT fk_link_ta_debet_account 
        FOREIGN KEY (debet_account_hk) 
        REFERENCES dv.hub_account(account_hk),
    CONSTRAINT fk_link_ta_credit_account 
        FOREIGN KEY (credit_account_hk) 
        REFERENCES dv.hub_account(account_hk)
);

COMMENT ON TABLE dv.link_transaction_account IS 'Линк связи транзакций и счетов (дебет/кредит)';
COMMENT ON COLUMN dv.link_transaction_account.link_transaction_account_hk IS 'Хэш-ключ связи (PK)';
COMMENT ON COLUMN dv.link_transaction_account.transaction_hk IS 'Хэш-ключ транзакции (FK)';
COMMENT ON COLUMN dv.link_transaction_account.debet_account_hk IS 'Хэш-ключ дебетового счета (FK)';
COMMENT ON COLUMN dv.link_transaction_account.credit_account_hk IS 'Хэш-ключ кредитового счета (FK)';
COMMENT ON COLUMN dv.link_transaction_account.load_date IS 'Дата загрузки связи';
COMMENT ON COLUMN dv.link_transaction_account.record_source IS 'Система-источник данных';

CREATE UNIQUE INDEX idx_link_ta_bk ON dv.link_transaction_account(transaction_hk, debet_account_hk, credit_account_hk);
CREATE INDEX idx_link_ta_transaction ON dv.link_transaction_account(transaction_hk);
CREATE INDEX idx_link_ta_debet ON dv.link_transaction_account(debet_account_hk);
CREATE INDEX idx_link_ta_credit ON dv.link_transaction_account(credit_account_hk);
CREATE INDEX idx_link_ta_load_date ON dv.link_transaction_account(load_date);

-- ========================================
-- ФУНКЦИИ УТИЛИТЫ
-- ========================================

-- Функция для генерации MD5 хэш-ключа
CREATE OR REPLACE FUNCTION dv.generate_hash_key(p_values TEXT[])
RETURNS CHAR(32) AS $$
BEGIN
    RETURN MD5(ARRAY_TO_STRING(p_values, '|'));
END;
$$ LANGUAGE plpgsql IMMUTABLE;

COMMENT ON FUNCTION dv.generate_hash_key(TEXT[]) IS 'Генерация MD5 хэша для бизнес-ключей';

-- Функция для генерации hash_diff
CREATE OR REPLACE FUNCTION dv.generate_hash_diff(p_values TEXT[])
RETURNS CHAR(32) AS $$
BEGIN
    RETURN MD5(ARRAY_TO_STRING(p_values, '|'));
END;
$$ LANGUAGE plpgsql IMMUTABLE;

COMMENT ON FUNCTION dv.generate_hash_diff(TEXT[]) IS 'Генерация MD5 хэша для отслеживания изменений атрибутов';

-- Функция для конвертации хэша в UUID
CREATE OR REPLACE FUNCTION dv.hash_to_uuid(p_hash TEXT)
RETURNS UUID AS $$
BEGIN
    RETURN (SUBSTRING(p_hash, 1, 8) || '-' || 
            SUBSTRING(p_hash, 9, 4) || '-' || 
            SUBSTRING(p_hash, 13, 4) || '-' || 
            SUBSTRING(p_hash, 17, 4) || '-' || 
            SUBSTRING(p_hash, 21, 12))::UUID;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

COMMENT ON FUNCTION dv.hash_to_uuid(TEXT) IS 'Конвертация MD5 хэша в формат UUID';

-- ========================================
-- VIEWS для упрощенного доступа
-- ========================================

-- View: Полная информация о клиентах (текущая версия)
CREATE OR REPLACE VIEW dv.v_customer_current AS
SELECT 
    hc.customer_hk,
    hc.customerid,
    sc.customername,
    hc.load_date as hub_load_date,
    sc.load_date as sat_load_date,
    hc.record_source
FROM dv.hub_customer hc
LEFT JOIN LATERAL (
    SELECT *
    FROM dv.sat_customer sc
    WHERE sc.customer_hk = hc.customer_hk
    ORDER BY sc.load_date DESC
    LIMIT 1
) sc ON TRUE;

COMMENT ON VIEW dv.v_customer_current IS 'Текущая версия данных клиентов';

-- View: Полная информация о счетах (текущая версия)
CREATE OR REPLACE VIEW dv.v_account_current AS
SELECT 
    ha.account_hk,
    ha.accountno,
    ha.currencyid,
    sa.accountname,
    sa.opendate,
    sa.closedate,
    ha.load_date as hub_load_date,
    sa.load_date as sat_load_date,
    ha.record_source
FROM dv.hub_account ha
LEFT JOIN LATERAL (
    SELECT *
    FROM dv.sat_account sa
    WHERE sa.account_hk = ha.account_hk
    ORDER BY sa.load_date DESC
    LIMIT 1
) sa ON TRUE;

COMMENT ON VIEW dv.v_account_current IS 'Текущая версия данных счетов';

-- View: Полная информация о транзакциях (текущая версия)
CREATE OR REPLACE VIEW dv.v_transaction_current AS
SELECT 
    ht.transaction_hk,
    ht.position,
    ht.positionn,
    st.currencyid,
    st.transactiondate,
    st.sumn,
    ht.load_date as hub_load_date,
    st.load_date as sat_load_date,
    ht.record_source
FROM dv.hub_transaction ht
LEFT JOIN LATERAL (
    SELECT *
    FROM dv.sat_transaction st
    WHERE st.transaction_hk = ht.transaction_hk
    ORDER BY st.load_date DESC
    LIMIT 1
) st ON TRUE;

COMMENT ON VIEW dv.v_transaction_current IS 'Текущая версия данных транзакций';

-- ========================================
-- Завершение скрипта
-- ========================================

-- Вывод информации о созданных объектах
DO $$
BEGIN
    RAISE NOTICE 'Data Vault структура успешно создана!';
    RAISE NOTICE 'Схема: dv';
    RAISE NOTICE 'Hubs: 3 (hub_customer, hub_account, hub_transaction)';
    RAISE NOTICE 'Satellites: 3 (sat_customer, sat_account, sat_transaction)';
    RAISE NOTICE 'Links: 2 (link_account_customer, link_transaction_account)';
    RAISE NOTICE 'Утилиты: 3 функции для работы с хэшами';
    RAISE NOTICE 'Views: 3 представления текущих данных';
END $$;
