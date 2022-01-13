--Обновим/добавим в DWH из STG данные по терминалам 
MERGE INTO de7.mshv_dwh_dim_terminals dwh
USING (
    select 
        * 
    from
        (select 
            terminal, 
            terminal_type,
            terminal_city,
            terminal_address,   
            ROW_NUMBER () over (PARTITION BY terminal order by trans_date DESC) as rn,
            trans_date,
            first_value (terminal) over (PARTITION BY terminal order by trans_date DESC) lv_tdate
        from de7.mshv_stg_transactions_inc) 
    WHERE rn = 1 ) stg
ON (dwh.terminal_id = stg.terminal)
WHEN MATCHED THEN UPDATE SET 
    dwh.terminal_type = stg.terminal_type,
    dwh.terminal_city = stg.terminal_city,
    dwh.terminal_address =stg.terminal_address
WHEN NOT MATCHED THEN INSERT ( 
    dwh.terminal_id, 
    dwh.terminal_type,
    dwh.terminal_city,
    dwh.terminal_address,
    dwh.create_dt
    ) 
VALUES (
	stg.terminal,
	stg.terminal_type,
	stg.terminal_city,
	stg.terminal_address,
    CURRENT_DATE
);
--обновление таблицы мета данных по терминалам
update de7.mshv_meta_table  m
set max_create_dt = (
    select max(create_dt) 
    from de7.mshv_dwh_dim_terminals
    )
where m.database_name = 'DE7'
and m.table_name = 'MSHV_DWH_DIM_TERMINALS';