--очистка stg таблицы по транзакциям за день
delete from de7.mshv_stg_transactions_inc;

--загрузка в stg таблицу выгрузки по транзакциям за день
insert into de7.mshv_stg_transactions_inc inc
select 
    *
from de7.mshv_stg_transactions stg
where stg.trans_date > (
    select m.max_create_dt 
    from de7.mshv_meta_table m 
    where m.database_name = 'DE7'
    and m.table_name = 'MSHV_STG_TRANSACTIONS_INC'
);
--обновление таблицы мета данных по транзакциям
update de7.mshv_meta_table  m
set max_create_dt = (
    select max(trans_date) 
    from de7.mshv_stg_transactions
    )
where m.database_name = 'DE7'
and m.table_name = 'MSHV_STG_TRANSACTIONS_INC';

--загрузка в DWH новых данных из STG по фактическим транзакциям 
MERGE INTO de7.mshv_dwh_fact_transactions dwh
USING de7.mshv_stg_transactions_inc stg
ON (dwh.trans_id = stg.trans_id)
WHEN MATCHED THEN UPDATE SET 
    dwh.trans_date = stg.trans_date,
    dwh.card_num = stg.card_num,
    dwh.oper_type=stg.oper_type,
    dwh.amt=stg.amt,
    dwh.oper_result=stg.oper_result,
    dwh.terminal=stg.terminal
WHEN NOT MATCHED THEN INSERT ( 
    dwh.trans_id, 
    dwh.trans_date,
    dwh.card_num,
    dwh.oper_type,
    dwh.amt,
    dwh.oper_result,
    dwh.terminal,
    dwh.create_dt
    ) 
VALUES (
	stg.trans_id,
	stg.trans_date,
	stg.card_num,
	stg.oper_type,
	stg.amt,
	stg.oper_result,
	stg.terminal,
    CURRENT_DATE
);

--обновление таблицы мета данных по транзакциям (до какой даты загружены данные)
update de7.mshv_meta_table  m
set max_create_dt = (
    select max(create_dt) 
    from de7.mshv_dwh_fact_transactions
    )
where m.database_name = 'DE7'
and m.table_name = 'MSHV_DWH_FACT_TRANSACTIONS';