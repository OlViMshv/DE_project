--Обновим/добавим в DWH из STG данные по картам
MERGE INTO de7.mshv_dwh_dim_cards dwh
USING de7.mshv_stg_transactions_inc stg
ON (stg.card_num = dwh.card_num)
WHEN MATCHED THEN UPDATE SET 
    dwh.account_num = stg.account_num
WHEN NOT MATCHED THEN INSERT ( 
    dwh.card_num, 
    dwh.account_num,
    dwh.create_dt
    ) 
VALUES (
	stg.card_num,
	stg.account_num,
    CURRENT_DATE
);
--обновление таблицы мета данных по картам
update de7.mshv_meta_table  m
set max_create_dt = (
    select max(create_dt) 
    from de7.mshv_dwh_dim_cards
    )
where m.database_name = 'DE7'
and m.table_name = 'MSHV_DWH_DIM_CARDS';