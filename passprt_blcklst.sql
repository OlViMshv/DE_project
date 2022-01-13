--загрузка в DWH таблицу выгрузки по заблокированным паспортам за день
MERGE INTO de7.mshv_dwh_fact_pssprt_blcklst
USING de7.mshv_stg_pssprt_blcklst
ON ( passport_num = passport )
WHEN NOT MATCHED THEN INSERT ( 
    passport_num, 
    entry_dt, 
    create_dt
    ) 
VALUES ( 
    passport, 
    start_dt, 
    CURRENT_DATE
);
--обновление таблицы мета данных по заблокированным паспортам
update de7.mshv_meta_table  m
set max_create_dt = (
    select max(create_dt) 
    from de7.mshv_dwh_fact_pssprt_blcklst
    )
where m.database_name = 'DE7'
and m.table_name = 'MSHV_DWH_FACT_PSSPRT_BLCKLST';