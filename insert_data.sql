-- Вставка тестовых данных в таблицы схемы stg_onlinebankdb

-- Вставка данных в таблицу Customers (клиенты)
INSERT INTO stg_onlinebankdb.dbo_customers (customerid, customername)
VALUES (1, 'Иванов Иван Иванович');

-- Вставка данных в таблицу Accounts (лицевые счета)
INSERT INTO stg_onlinebankdb.dbo_accounts (accountno, currencyid, customerid, accountname, opendate, closedate)
VALUES ('40817810123456789012', 810, 1, 'Счет для зарплаты', '2024-01-15', NULL);

-- Вставка данных в таблицу Transactions (транзакции)
INSERT INTO stg_onlinebankdb.dbo_transactions (position, positionn, currencyid, transactiondate, debetaccountno, creditaccountno, sumn)
VALUES (1, 1, 810, '2024-02-01', '40817810123456789012', '40702810987654321098', 50000.00);
