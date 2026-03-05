-- Создание схемы stg_onlinebankdb
CREATE SCHEMA IF NOT EXISTS stg_onlinebankdb;

-- Таблица Accounts (лицевые счета)
CREATE TABLE stg_onlinebankdb.dbo_accounts (
    accountno TEXT NOT NULL,
    currencyid INT NOT NULL,
    customerid INT NULL,
    accountname TEXT NULL,
    opendate DATE NULL,
    closedate DATE NULL,
    PRIMARY KEY (accountno, currencyid)
);

-- Таблица Customers (клиенты)
CREATE TABLE stg_onlinebankdb.dbo_customers (
    customerid INT NOT NULL PRIMARY KEY,
    customername TEXT NULL
);

-- Таблица Transactions (транзакции)
CREATE TABLE stg_onlinebankdb.dbo_transactions (
    position BIGINT NOT NULL,
    positionn SMALLINT NOT NULL,
    currencyid INT NULL,
    transactiondate DATE NULL,
    debetaccountno TEXT NULL,
    creditaccountno TEXT NULL,
    sumn NUMERIC(15, 2) NULL,
    PRIMARY KEY (position, positionn)
);

-- Добавление внешних ключей
ALTER TABLE stg_onlinebankdb.dbo_accounts
ADD CONSTRAINT fk_accounts_customer
FOREIGN KEY (customerid) REFERENCES stg_onlinebankdb.dbo_customers(customerid);

-- Комментарии к колонкам таблицы Accounts
COMMENT ON COLUMN stg_onlinebankdb.dbo_accounts.accountno IS 'номер счета';
COMMENT ON COLUMN stg_onlinebankdb.dbo_accounts.currencyid IS 'id валюты';
COMMENT ON COLUMN stg_onlinebankdb.dbo_accounts.customerid IS 'id клиента';
COMMENT ON COLUMN stg_onlinebankdb.dbo_accounts.accountname IS 'наименование счета';
COMMENT ON COLUMN stg_onlinebankdb.dbo_accounts.opendate IS 'дата открытия счета';
COMMENT ON COLUMN stg_onlinebankdb.dbo_accounts.closedate IS 'дата закрытия счета';

-- Комментарии к колонкам таблицы Customers
COMMENT ON COLUMN stg_onlinebankdb.dbo_customers.customerid IS 'id клиента';
COMMENT ON COLUMN stg_onlinebankdb.dbo_customers.customername IS 'имя клиента';

-- Комментарии к колонкам таблицы Transactions
COMMENT ON COLUMN stg_onlinebankdb.dbo_transactions.position IS 'порядковый номер проводки';
COMMENT ON COLUMN stg_onlinebankdb.dbo_transactions.positionn IS 'порядковый номер подпроводки';
COMMENT ON COLUMN stg_onlinebankdb.dbo_transactions.currencyid IS 'id валюты';
COMMENT ON COLUMN stg_onlinebankdb.dbo_transactions.transactiondate IS 'дата транзакции';
COMMENT ON COLUMN stg_onlinebankdb.dbo_transactions.debetaccountno IS 'номер кредитового лицевого счета';
COMMENT ON COLUMN stg_onlinebankdb.dbo_transactions.creditaccountno IS 'номер дебетового лицевого счета';
COMMENT ON COLUMN stg_onlinebankdb.dbo_transactions.sumn IS 'сумма транзакции';

-- Индексы для оптимизации запросов
CREATE INDEX idx_accounts_customerid ON stg_onlinebankdb.dbo_accounts(customerid);
CREATE INDEX idx_transactions_debetaccountno ON stg_onlinebankdb.dbo_transactions(debetaccountno, currencyid);
CREATE INDEX idx_transactions_creditaccountno ON stg_onlinebankdb.dbo_transactions(creditaccountno, currencyid);
CREATE INDEX idx_transactions_date ON stg_onlinebankdb.dbo_transactions(transactiondate);
