#!/usr/bin/env python
# coding: utf-8

# In[29]:


# Код для импорта из Excel в Orcale транзакций (рус)
import cx_Oracle
import pandas
import os, fnmatch
from sqlalchemy import types, create_engine

listOfFiles = os.listdir('.')  
pattern = "transactions_*.xlsx"
for entry in listOfFiles:  
    if fnmatch.fnmatch(entry, pattern):
        filename = entry
        file = open(filename, encoding ="utf-8" )        
        df = pandas.read_excel(filename)
        df.columns = ['TRANS_DATE' if x=='date' else x for x in df.columns]
        dtyp = {c:types.VARCHAR(df[c].str.len().max())
            for c in df.columns[df.dtypes == 'object'].tolist()}
     
        con = create_engine( 'oracle+cx_oracle://de7:meadow@de.chronosavant.ru:1521/deoracle' )
        df.to_sql('mshv_stg_transactions', con, schema='DE7', dtype=dtyp, if_exists='replace', index = False)
        file.close()
        new_filename = filename + '.' + 'backup'
        os.rename(filename, new_filename)


# In[30]:


# Код для импорта из Excel в Orcale паспорта
import cx_Oracle
import pandas
import os, fnmatch
from sqlalchemy import types, create_engine

listOfFiles = os.listdir('.')  
pattern = "passports_blacklist_*.xlsx"  
for entry in listOfFiles:  
    if fnmatch.fnmatch(entry, pattern):
        filename = entry
        df = pandas.read_excel(filename)
        con = create_engine( 'oracle+cx_oracle://de7:meadow@de.chronosavant.ru:1521/deoracle' )
        df.to_sql('mshv_stg_pssprt_blcklst', con, schema='DE7', if_exists='replace', index = False)
        file.close()
        new_filename = filename + '.' + 'backup'
        os.rename(filename, new_filename)


# In[31]:


# Заполнение DWH таблиц
import cx_Oracle

con=cx_Oracle.connect('de7/meadow@de.chronosavant.ru/deoracle')

cur=con.cursor()
#загрузка в DWH таблицу выгрузки по заблокированным паспортам за день
cur.execute ("""MERGE INTO de7.mshv_dwh_fact_pssprt_blcklst
                USING de7.mshv_stg_pssprt_blcklst
                ON (passport_num = passport)
                WHEN NOT MATCHED THEN INSERT (
                    passport_num, 
                    entry_dt, 
                    create_dt
                )
                VALUES (
                    passport, 
                    start_dt, 
                    CURRENT_DATE
                )""")
# обновление таблицы мета данных по заблокированным паспортам
cur.execute ("""update de7.mshv_meta_table  m
                set max_create_dt = (
                    select max(create_dt) 
                    from de7.mshv_dwh_fact_pssprt_blcklst
                    )
                where m.database_name = 'DE7'
                and m.table_name = 'MSHV_DWH_FACT_PSSPRT_BLCKLST' """)
# очистка stg таблицы по транзакциям за день
cur.execute ("""delete from de7.mshv_stg_transactions_inc""")
# загрузка в stg таблицу выгрузки по транзакциям за день
cur.execute ("""insert into de7.mshv_stg_transactions_inc inc
                select 
                    *
                from de7.mshv_stg_transactions stg
                where stg.trans_date > (
                    select m.max_create_dt 
                    from de7.mshv_meta_table m 
                    where m.database_name = 'DE7'
                    and m.table_name = 'MSHV_STG_TRANSACTIONS_INC'
                    )""")
# обновление таблицы мета данных по транзакциям
cur.execute ("""update de7.mshv_meta_table  m
                set max_create_dt = (
                    select max(trans_date) 
                    from de7.mshv_stg_transactions
                    )
                where m.database_name = 'DE7'
                and m.table_name = 'MSHV_STG_TRANSACTIONS_INC'""")
# загрузка в DWH новых данных из STG по фактическим транзакциям 
cur.execute ("""MERGE INTO de7.mshv_dwh_fact_transactions dwh
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
                    CURRENT_DATE)
                    """)

cur.execute ("""update de7.mshv_meta_table  m
                set max_create_dt = (
                    select max(create_dt) 
                    from de7.mshv_dwh_fact_transactions
                    )
                where m.database_name = 'DE7'
                and m.table_name = 'MSHV_DWH_FACT_TRANSACTIONS'""")
# Обновим/добавим в DWH из STG данные по терминалам 
cur.execute ("""MERGE INTO de7.mshv_dwh_dim_terminals dwh
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
                    )""")
# обновление таблицы мета данных по терминалам
cur.execute ("""update de7.mshv_meta_table  m
                set max_create_dt = (
                    select max(create_dt) 
                    from de7.mshv_dwh_dim_terminals
                    )
                where m.database_name = 'DE7'
                and m.table_name = 'MSHV_DWH_DIM_TERMINALS'""")
# Обновим/добавим в DWH из STG данные по картам
cur.execute ("""MERGE INTO de7.mshv_dwh_dim_cards dwh
                USING de7.mshv_stg_transactions_inc stg
                ON (dwh.card_num = stg.card_num)
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
                    )""")
# обновление таблицы мета данных по картам
cur.execute ("""update de7.mshv_meta_table  m
                set max_create_dt = (
                    select max(create_dt) 
                    from de7.mshv_dwh_dim_cards
                    )
                where m.database_name = 'DE7'
                and m.table_name = 'MSHV_DWH_DIM_CARDS'""")
# Обновим/добавим в DWH из STG данные по счетам
cur.execute ("""MERGE INTO de7.mshv_dwh_dim_accounts dwh
                USING de7.mshv_stg_transactions_inc stg
                ON (dwh.account_num = stg.account_num)
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
                )""")
# обновление таблицы мета данных по счетам
cur.execute ("""update de7.mshv_meta_table  m
                set max_create_dt = (
                    select max(create_dt) 
                    from de7.mshv_dwh_dim_accounts
                    )
                where m.database_name = 'DE7'
                and m.table_name = 'MSHV_DWH_DIM_ACCOUNTS'""")
# Обновим/добавим в DWH из STG данные по клиентам
cur.execute ("""MERGE INTO de7.mshv_dwh_dim_clients dwh
                USING 
                (
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
                    from de7.mshv_stg_transactions_inc) 
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
                    )"""
         )
# обновление таблицы мета данных по клиентам
cur.execute ("""update de7.mshv_meta_table  m
                set max_create_dt = (
                    select max(create_dt) 
                    from de7.mshv_dwh_dim_clients
                    )
                where m.database_name = 'DE7'
                and m.table_name = 'MSHV_DWH_DIM_CLIENTS'""")

con.commit() 
con.close()


# In[32]:


# поиск мошеннических операций и постоение отчета
import cx_Oracle

con=cx_Oracle.connect('de7/meadow@de.chronosavant.ru/deoracle')

cur=con.cursor()

# совершение операций при просроченных или заблокированных паспортах
cur.execute ("""insert INTO de7.mshv_rep_fraud 
                select 
                    * 
                from 
                (
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
                    WHERE rn = 1
                    ) 
                )"""
            )

# совершение операций при недействующем договоре
cur.execute ("""insert INTO de7.mshv_rep_fraud 
                select 
                    * 
                from 
                (
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
                )"""
            )
# Совершение операции в разных городах в течение часа
cur.execute ("""insert INTO de7.mshv_rep_fraud
                select 
                    * 
                from 
                    (
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
                        where city > 1
                        and rn = 1
                    )"""
            )

con.commit() 
con.close()

