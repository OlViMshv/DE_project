--Обновим/добавим в DWH из STG данные по клиентам
MERGE INTO de7.mshv_dwh_dim_clients dwh
USING (
    select 
        * 
    from
        (select
            client,
            ROW_NUMBER () over (PARTITION BY client order by trans_date DESC) as rn,
            trans_date,
            first_value (client) over (PARTITION BY client order by trans_date DESC) lv_tdate,
            last_name,
            first_name,
            patronymic,
            date_of_birth,
            passport_num,
            passport_valid_to,
            phone
        from de7.mshv_stg_transactions_inc
        ) 
    WHERE rn = 1 
    ) stg
ON (dwh.client_id = stg.client )
WHEN MATCHED THEN UPDATE SET 
    dwh.last_name = stg.last_name,
    dwh.first_name = stg.first_name,
    dwh.patronymic = stg.patronymic,
    dwh.passport_num = stg.passport_num,
    dwh.passport_valid_to = stg.passport_valid_to, 
    dwh.phone = stg.phone
WHEN NOT MATCHED THEN INSERT ( 
    dwh.client_id,
    dwh.last_name,
    dwh.first_name,
    dwh.patronymic,
    dwh.date_of_birth,
    dwh.passport_num,
    dwh.passport_valid_to, 
    dwh.phone,
    dwh.create_dt
    ) 
VALUES (
    stg.client, 
    stg.last_name, 
    stg.first_name,
    stg.patronymic,
    stg.date_of_birth,
    stg.passport_num,
    stg.passport_valid_to,
    stg.phone,
    CURRENT_DATE
);
--обновление таблицы мета данных по клиентам
update de7.mshv_meta_table  m
set max_create_dt = (
    select max(create_dt) 
    from de7.mshv_dwh_dim_clients
    )
where m.database_name = 'DE7'
and m.table_name = 'MSHV_DWH_DIM_CLIENTS';