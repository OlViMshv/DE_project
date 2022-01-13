--удаление всех таблиц
drop table de7.mshv_stg_transactions_inc;

drop table de7.mshv_dwh_dim_terminals;
drop table de7.mshv_dwh_dim_cards;
drop table de7.mshv_dwh_dim_accounts;
drop table de7.mshv_dwh_dim_clients;

drop table de7.mshv_dwh_fact_transactions;
drop table de7.mshv_dwh_fact_pssprt_blcklst;

drop table de7.mshv_meta_table;
drop table de7.mshv_rep_fraud;

--создание таблиц
create table de7.mshv_dwh_dim_terminals (
	terminal_id VARCHAR2 (50),
	terminal_type VARCHAR2 (10),
	terminal_city VARCHAR2 (200),
	terminal_address VARCHAR2 (200),
	create_dt DATE,
	update_dt DATE
);

create table de7.mshv_dwh_dim_cards (
	card_num VARCHAR2 (50),
	account_num VARCHAR2 (50),
	create_dt DATE,
	update_dt DATE
);

create table de7.mshv_dwh_dim_accounts (
	account_num VARCHAR2 (50),
	valid_to DATE,
	client VARCHAR2 (10),
	create_dt DATE,
	update_dt DATE
);

create table de7.mshv_dwh_dim_clients (
	client_id VARCHAR2 (20),
	last_name VARCHAR2 (200),
	first_name VARCHAR2 (200),
	patronymic VARCHAR2 (200),
	date_of_birth DATE,
	passport_num VARCHAR2 (50),
	passport_valid_to DATE, 
	phone VARCHAR2 (50),
	create_dt DATE,
	update_dt DATE
);

create table de7.mshv_dwh_fact_transactions (
	trans_id VARCHAR2 (20),
	trans_date DATE,
	card_num VARCHAR2 (50),
	oper_type VARCHAR2 (50),
	amt decimal,
	oper_result VARCHAR2 (50),
	terminal VARCHAR2 (50),
	create_dt DATE
);

create table de7.mshv_dwh_fact_pssprt_blcklst (
	passport_num VARCHAR2 (10),
	entry_dt DATE,
	create_dt DATE
);

--создание STG таблиц для импорта из excel 
create table de7.mshv_stg_transactions_inc(
	trans_id VARCHAR2 (20),
	trans_date DATE,
	card_num VARCHAR2 (50),
	account_num VARCHAR2 (200),
	account_valid_to DATE,
	client VARCHAR2 (20),
	last_name VARCHAR2 (200),
	first_name VARCHAR2 (200),
	patronymic VARCHAR2 (200),
	date_of_birth DATE,
	passport_num VARCHAR2 (50),
	passport_valid_to DATE, 
	phone VARCHAR2 (50),
	oper_type VARCHAR2 (200),
	amt float,
	oper_result VARCHAR2 (200),
	terminal VARCHAR2 (50),
	terminal_type VARCHAR2 (10),
	terminal_city VARCHAR2 (200),
	terminal_address VARCHAR2 (200)
);

--создание таблицы с отчетом
create table de7.mshv_rep_fraud (
	event_dt DATE,
	passport VARCHAR2 (50),
	fio VARCHAR2 (200),
	phone VARCHAR2 (50),
	event_type VARCHAR2 (200),
	report_dt DATE
);

--создание мета таблицы
create table de7.mshv_meta_table (
	database_name varchar2(30), 
	table_name varchar2(30), 
	max_create_dt date
);

--вставить в таблицу метаданных первое значение
insert into de7.mshv_meta_table (
	database_name, 
	table_name, 
	max_create_dt
) 
VALUES (
	'DE7', 
	'MSHV_DWH_FACT_PSSPRT_BLCKLST', 
	to_date ('1900-01-01', 'YYYY-MM-DD')
);
insert into de7.mshv_meta_table (
	database_name, 
	table_name, 
	max_create_dt
) 
VALUES (
	'DE7', 
	'MSHV_DWH_FACT_TRANSACTIONS', 
	to_date ('1900-01-01', 'YYYY-MM-DD')
);
insert into de7.mshv_meta_table (
	database_name, 
	table_name, 
	max_create_dt
) 
VALUES (
	'DE7', 
	'MSHV_DWH_DIM_CLIENTS', 
	to_date ('1900-01-01', 'YYYY-MM-DD')
);
insert into de7.mshv_meta_table (
	database_name, 
	table_name, 
	max_create_dt
) 
VALUES (
	'DE7', 
	'MSHV_DWH_DIM_ACCOUNTS', 
	to_date ('1900-01-01', 'YYYY-MM-DD')
);
insert into de7.mshv_meta_table (
	database_name, 
	table_name, 
	max_create_dt
) 
VALUES (
	'DE7', 
	'MSHV_DWH_DIM_CARDS', 
	to_date ('1900-01-01', 'YYYY-MM-DD')
);
insert into de7.mshv_meta_table (
	database_name, 
	table_name, 
	max_create_dt
) 
VALUES (
	'DE7', 
	'MSHV_DWH_DIM_TERMINALS', 
	to_date ('1900-01-01', 'YYYY-MM-DD')
);
insert into de7.mshv_meta_table (
	database_name, 
	table_name, 
	max_create_dt
) 
VALUES (
	'DE7', 
	'MSHV_STG_TRANSACTIONS_INC', 
	to_date ('1900-01-01', 'YYYY-MM-DD')
);

commit;