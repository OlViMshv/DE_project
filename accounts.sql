--Обновим/добавим в DWH из STG данные по счетам
MERGE INTO de7.mshv_dwh_dim_accounts dwh
USING de7.mshv_stg_transactions_inc stg
ON (stg.account_num = dwh.account_num)
WHEN MATCHED THEN UPDATE SET 
    dwh.valid_to = stg.account_valid_to,
    dwh.client = stg.client
WHEN NOT MATCHED THEN INSERT ( 
    dwh.account_num, 
    dwh.valid_to,
    dwh.client,
    dwh.create_dt
    ) 
VALUES (
    stg.account_num,
	stg.account_valid_to,
	stg.client,
    CURRENT_DATE
);
--обновление таблицы мета данных по счетам
update de7.mshv_meta_table  m
set max_create_dt = (
    select max(create_dt) 
    from de7.mshv_dwh_dim_accounts
    )
where m.database_name = 'DE7'
and m.table_name = 'MSHV_DWH_DIM_ACCOUNTS';