--1. просроченные или заблокированные паспорта
insert INTO de7.mshv_rep_fraud 
select 
    * 
from (
    (SELECT 
        trans_date, 
        passport_num, 
        fio, 
        phone,
        'Операции при просроченном или заблокированном паспорте',
        CURRENT_DATE
    from (
        select
            ft.trans_date,
            ROW_NUMBER () over (PARTITION BY da.client order by ft.trans_id DESC) as rn,
            first_value (da.client) over (PARTITION BY da.client order by ft.trans_id DESC) client,
            cl.last_name ||' '|| cl.first_name ||' '|| cl.patronymic fio,
            cl.phone,
            cl.passport_num,
            cl.passport_valid_to,
            pb.create_dt as datapb
        from de7.mshv_dwh_fact_transactions ft
        left JOIN de7.mshv_dwh_dim_cards dc
        on ft.card_num = dc.card_num
        left JOIN de7.mshv_dwh_dim_accounts da
        on dc.account_num = da.account_num
        LEFT JOIN de7.mshv_dwh_dim_clients cl
        on da.client=cl.client_id
        left JOIN de7.mshv_dwh_fact_pssprt_blcklst pb
        on cl.passport_num = pb.passport_num
        where 
        ft.trans_date > cl.passport_valid_to+1
        or pb.create_dt is not null
        and to_date (to_char (ft.trans_date,'DD.MM.YYYY'), 'DD.MM.YYYY')
            > = (select to_date (to_char (m.max_create_dt, 'DD.MM.YYYY'), 'DD.MM.YYYY') max_create_dt from de7.mshv_meta_table m 
            where database_name = 'DE7'
            and table_name = 'MSHV_STG_TRANSACTIONS_INC')
        ) 
    WHERE rn = 1) 
);

-- 2. совершение операций при недействующем договоре
insert INTO de7.mshv_rep_fraud 
select 
    * 
from (
    (SELECT 
        trans_date, 
        passport_num, 
        fio, 
        phone,
        'Операции при недействующем договоре',
        CURRENT_DATE
    from (
        select distinct
            ft.trans_date, 
            first_value (da.client) over (PARTITION BY da.client order by ft.trans_id DESC) client,
            da.valid_to,
            cl.last_name ||' '|| cl.first_name ||' '|| cl.patronymic fio,
            cl.passport_num,
            cl.phone,
            ft.create_dt
        from de7.mshv_dwh_fact_transactions ft
        left JOIN de7.mshv_dwh_dim_cards dc
        on ft.card_num = dc.card_num
        left JOIN de7.mshv_dwh_dim_accounts da
        on dc.account_num = da.account_num
        LEFT JOIN de7.mshv_dwh_dim_clients cl
        on da.client=cl.client_id
        where ft.trans_date > da.valid_to
        and to_char (ft.trans_date,'DD.MM.YYYY')
                > = (select to_char (m.max_create_dt, 'DD.MM.YYYY') max_create_dt from de7.mshv_meta_table m 
                where database_name = 'DE7'
                and table_name = 'MSHV_STG_TRANSACTIONS_INC')
        )
    ) 
    );

-- 3. Операции в разных городах в течение часа
insert INTO de7.mshv_rep_fraud 
select 
    * 
from (
    select
        trans_date, 
        passport_num, 
        fio, 
        phone,
        'Операции в разных городах в течение часа',
        CURRENT_DATE
        from (
            select
            count(distinct q.terminal_city)over (PARTITION BY q.client_id, q.hh) city,
            ROW_NUMBER () over (PARTITION BY q.client_id, q.hh order by TRANS_DATE desc) as rn,
            q.client_id, q.hh,q.fio,TRANS_DATE,terminal_city, phone,passport_num
            from
                (select distinct
                TRANS_DATE ,
                to_char (trans_date,'DD.MM.YYYY HH24') hh,
                dt.terminal_city,
                cl.client_id,
                cl.last_name ||' '|| cl.first_name ||' '|| cl.patronymic fio,
                cl.phone,
                cl.passport_num
                from de7.mshv_dwh_fact_transactions ft
                left JOIN de7.mshv_dwh_dim_terminals dt
                on ft.terminal = dt.terminal_id
                left JOIN de7.mshv_dwh_dim_cards dc
                on ft.card_num = dc.card_num
                left JOIN de7.mshv_dwh_dim_accounts da
                on dc.account_num = da.account_num
                LEFT JOIN de7.mshv_dwh_dim_clients cl
                on da.client=cl.client_id)
            q)
        where city >1
        and rn = 1);