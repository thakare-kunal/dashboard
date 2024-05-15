from airflow.decorators import dag, task
from datetime import datetime
import logging

import pandas as pd
import numpy as np
import time
import math
import warnings
warnings.filterwarnings("ignore")

import concurrent.futures
import mysql.connector
import configparser
import mysql.connector.pooling

conf = configparser.ConfigParser()
conf_state = configparser.ConfigParser()
conf.read(r"./configure.yaml")
conf_state.read(r"./db_state.yaml")

# SOURCE_CONNECTION_LOCAL = mysql.connector.connect(
#     host = conf.get("source", "_host"),
#     user = conf.get("source", "_username"),
#     password = conf.get("source", "_password"),
#     database = conf.get("source", "_database")
# )

# DESTINATION_CONNECTION_LOCAL = mysql.connector.connect(
#     host = conf.get("destination", "_host"),
#     user = conf.get("destination", "_username"),
#     password = conf.get("destination", "_password"),
#     database = conf.get("destination", "_database")
# )

THREAD_COUNT = int(conf.get("app", "thread_count"))

dbconfig_destination_local = {
            "host" : conf.get("destination", "_host"),
            "user" : conf.get("destination", "_username"),
            "password" : conf.get("destination", "_password"),
            "database" : conf.get("destination", "_database")
    }

dbconfig_source_local = {
            "host" : conf.get("source", "host"),
            "user" : conf.get("source", "username"),
            "password" : conf.get("source", "password"),
            "database" : conf.get("source", "database")
    }

connection_pool_destination = mysql.connector.pooling.MySQLConnectionPool(pool_name="mypool", pool_size=6, **dbconfig_destination_local)
connection_pool_source = mysql.connector.pooling.MySQLConnectionPool(pool_name="mypool", pool_size=6, **dbconfig_source_local)

table = conf.get("destination", "table")

none = None
null = None
nan = None
NaN = None

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='./airflow_logs.log'
)

def get_pool_connections(pool):
    active_connections = sum(1 for conn in pool._idle_connections if conn.is_connected())  # Count idle connections that are still connected
    total_connections = pool.pool_size
    return active_connections, total_connections

def delete_chunk(chunk, connection):
    cursor = connection.cursor()
    delete_query = f"""delete from {table} where sale_order_id in {chunk}"""
    cursor.execute(delete_query)
    cursor.close()

def fetch_chunk(chunk, connection):
    q = f"""
        SELECT 
            /* sale order item details */
            soi.id AS sale_order_item_id,
            soi.itemParentable_type,
            soi.itemParentable_id,
            soi.saleable_type,
            soi.saleable_id,
            soi.discount_percentage AS sale_order_item_discount_percentage,
            soi.mrp AS sale_order_item_mrp,
            soi.unit_price_with_tax,
            soi.unit_price_without_tax,
            soi.cgst_tax_rate,
            soi.sgst_tax_rate,
            soi.igst_tax_rate,
            soi.cgst_tax_amount,
            soi.sgst_tax_amount,
            soi.igst_tax_amount,
            soi.is_dc,
            
            /* sale order details */
            so.id AS sale_order_id,
            so.sale_order_date,
            so.net_total AS sale_order_net_total,
            so.sales_channel,
            so.state AS order_state,
    
            /* Product Details */
            (
                CASE
                    WHEN cb.id IS NULL AND t.name IS NOT NULL THEN t.name
                    WHEN cb.id IS NOT NULL THEN cb.name
                END
            ) AS title_name,
            e.name AS edition_name,
            ept.ref_code,
            mtt.name AS master_title_type,
            mtst.name AS master_title_sub_type,
            (
                CASE
                    WHEN soi.itemParentable_type LIKE "%StandardMetaData" THEN smd.abbr
                    WHEN soi.itemParentable_type LIKE "%NotebookTitleDetail" THEN "Notebook"
                    WHEN cb.id IS NOT NULL THEN smd2.abbr
                    ELSE NULL
                END
            ) AS meta_data,
    
            /* invoice item details */
            ii.id AS invoice_item_id,
            soi.quantity,
            COALESCE(ii.quantity, 0) AS ii_quantity,
            ii.is_bill AS invoice_item_is_bill,
            ii.is_dc AS invoice_item_is_dc,
            (
            	CASE
                	WHEN ii.invoice_id is not null then 1
                	ELSE 0
                END
            ) AS is_invoiced,
     
            /* Invoice Details */
            i.id AS invoice_id,
            i.prefix AS invoice_prefix,
            i.is_bill AS invoice_is_bill,
            i.cr_dr_state AS invoice_cr_dr_state,
            i.state AS invoice_state,
            i.round_off AS invoice_round_off,
            i.date AS invoice_date,
            COALESCE(i.net_total, 0) AS invoice_total_with_tax,
            ( COALESCE(ii.quantity, 0)  * soi.unit_price_without_tax ) AS total_unit_price_without_tax,
            advance2.advance_amount AS advance_amount,
            p_mode.cash_amount,
            p_mode.cheque_amount,
            p_mode.online_amount,
            
            /* Customer Details */
            so.contactable_type,
            so.contactable_id,
            c.billing_name AS customer_name,
            crs.name as customer_category,
            so.billing_address, 
            
            /* RSM */
        	so.branch_id,
        	b.name AS branch_name,
        	b.sales_group_id,
        	sg.name AS sales_group_name,
        	CONCAT(cid.salutation, " ", cid.first_name, " ", cid.last_name) AS rsm,
    
            /* TAT */
            s_tat.packed_2_delivered_tat_day,
            /* s_tat.packed_2_delivered_tat_minutes, */
            so_tat.create_2_processing_tat_day,
            /* so_tat.create_2_processing_tat_minutes, */
            so_tat.processing_2_complete_tat_day,
            /* so_tat.processing_2_complete_tat_minutes, */
            
            now() as fetch_date /* DATE_ADD(NOW(), INTERVAL '5:30' HOUR_MINUTE) */
    
        /* Sale Order and Invoice Items Tables */
        FROM sale_order_items as soi 
        inner join sale_orders as so on so.id = soi.sale_order_id
        left join invoice_items as ii on ii.sale_order_item_id = soi.id
    
        /* Invoice Tables */
        LEFT JOIN invoices AS i ON ii.invoice_id = i.id
    
        /* Advance Amount */
        left join ( 
        SELECT 
            i.id AS invoice_id, SUM(cr_dr.amount) AS advance_amount 
        FROM invoices AS i 
        INNER JOIN ledger_transactions AS l1 ON l1.ledgerable_id = i.id AND l1.ledgerable_type LIKE "%Invoice" AND l1.state = "ACTIVE"
        INNER JOIN credit_debit_mappings AS cr_dr ON cr_dr.debit_id = l1.id
        INNER JOIN ledger_transactions AS l2 ON l2.id = cr_dr.credit_id AND l2.ledgerable_type LIKE "%Payment"  AND l2.state = "ACTIVE"
        INNER JOIN payments AS p ON p.id = l2.ledgerable_id
        WHERE p.cr_dr_state IN ("SETTLED", "PARTIAL_SETTLED") AND l1.transaction_datetime > l2.transaction_datetime
        GROUP by i.id ) as advance2 on advance2.invoice_id = i.id
    
        /* Payment Mode */
        LEFT JOIN ( SELECT i.id AS invoice_id,
        SUM( CASE WHEN p.mode = "CASH" THEN cr_dr.amount ELSE 0 END ) as cash_amount,
        SUM( CASE WHEN p.mode = "CHEQUE" THEN cr_dr.amount ELSE 0 END ) as cheque_amount,
        SUM( CASE WHEN p.mode = "ONLINE" THEN cr_dr.amount ELSE 0 END ) as online_amount
        FROM invoices AS i 
        INNER JOIN ledger_transactions AS l1 ON l1.ledgerable_id = i.id AND l1.ledgerable_type LIKE "%Invoice" AND l1.state = "ACTIVE" 
        INNER JOIN credit_debit_mappings AS cr_dr ON cr_dr.debit_id = l1.id 
        INNER JOIN ledger_transactions AS l2 ON l2.id = cr_dr.credit_id AND l2.ledgerable_type LIKE "%Payment" AND l2.state = "ACTIVE" 
        INNER JOIN payments AS p ON p.id = l2.ledgerable_id WHERE p.cr_dr_state IN ("SETTLED", "PARTIAL_SETTLED") 
        GROUP by i.id ) as p_mode on p_mode.invoice_id = i.id
    
        /* TAT */
        /*Shipment TAT*/
        LEFT JOIN ( 
            SELECT 
                s.sale_order_id, 
                MIN(DATEDIFF(sl2.created_at, sl1.created_at)) AS packed_2_delivered_tat_day 
                /* MIN(TIMEDIFF(sl2.created_at, sl1.created_at)) AS packed_2_delivered_tat_minutes  */
            FROM shipments AS s 
            LEFT JOIN shipment_state_logs AS sl1 ON sl1.shipment_id = s.id AND sl1.state LIKE "PACKED" 
            LEFT JOIN shipment_state_logs AS sl2 ON sl2.shipment_id = s.id AND sl2.state LIKE "DELIVERED" 
            WHERE s.created_at > "2022/10/01" 
            GROUP BY s.sale_order_id 
            HAVING packed_2_delivered_tat_day IS NOT NULL) AS s_tat on s_tat.sale_order_id = so.id
    
        /* Sale Order TAT */
        LEFT JOIN ( 
            SELECT 
                so.id as sale_order_id, 
                MIN(DATEDIFF(c2p2.created_at,c2p1.created_at)) as create_2_processing_tat_day, 
                /* MIN(TIMEDIFF(c2p2.created_at, c2p1.created_at)) as create_2_processing_tat_minutes, */
                MIN(DATEDIFF(p2c2.created_at,p2c1.created_at)) as processing_2_complete_tat_day 
                /* MIN(TIMEDIFF(p2c2.created_at, p2c1.created_at)) as processing_2_complete_tat_minutes */
            FROM sale_orders as so 
            left join sale_order_state_logs as c2p1 on c2p1.sale_order_id = so.id and c2p1.state in ("CREATED")
            left join sale_order_state_logs as c2p2 on c2p2.sale_order_id = so.id and c2p2.state in ("SEND_FOR_DIGITAL", "PROCESSED")
            left join sale_order_state_logs as p2c1 on p2c1.sale_order_id = so.id and p2c1.state in ("SEND_FOR_DIGITAL", "PROCESSED")
            left join sale_order_state_logs as p2c2 on p2c2.sale_order_id = so.id and p2c2.state in ("COMPLETED") 
            where so.state in ("COMPLETED", "FORCED_COMPLETED") and so.created_at > "2022/10/01"
            group by so.id
            HAVING create_2_processing_tat_day is not null ) as so_tat on so_tat.sale_order_id = so.id
    
        /* Product Tables */
        LEFT JOIN edition_product_types AS ept on ept.id = soi.saleable_id
        LEFT JOIN editions AS e ON e.id = ept.edition_id
        LEFT JOIN titles as t on t.id = e.title_id
        LEFT JOIN master_title_types as mtt on mtt.id = t.master_title_type_id
        LEFT JOIN master_title_sub_types as mtst on mtst.id = t.master_title_sub_type_id
        LEFT JOIN standard_meta_data as smd on smd.id = soi.itemParentable_id AND soi.itemParentable_type LIKE "%StandardMetaData" AND soi.saleable_type like "%EditionProductType"
        LEFT JOIN notebook_title_details as ntd on ntd.id = soi.itemParentable_id and soi.itemParentable_type LIKE "%NotebookTitleDetail"
        LEFT JOIN customized_books as cb on cb.id = soi.saleable_id and soi.saleable_type LIKE "%CustomizedBook"
        LEFT JOIN standard_meta_data as smd2 on smd2.id = cb.standard_meta_data_id
    
        /* RSM Tables */
        LEFT JOIN branches as b on so.branch_id = b.id
        LEFT JOIN sales_groups as sg on b.sales_group_id = sg.id
        LEFT JOIN employees as emp on sg.rsm = emp.id
        LEFT JOIN contact_individual_details as cid on cid.contact_id = emp.contact_id
    
        /* Customer Tables */
        LEFT JOIN customers as c on c.id = so.contactable_id AND so.contactable_type LIKE "%Customer"
        LEFT JOIN c_roles as crs on crs.id = c.role_id
    
        /* Filters Tables */
        WHERE ( soi.saleable_type LIKE "%EditionProductType"  ) AND ( so.state in ("PARTIAL_COMPLETED", "COMPLETED", "FORCED_COMPLETED", "CANCELLED")) AND so.id in {chunk}
        """
    df = pd.read_sql_query(q, connection)
    return(df)

def data_transform(df):
    df["itemParentable_type"] = df["itemParentable_type"].apply(lambda x: x.split("\\")[-1] if x is not None else None )
    df["saleable_type"] = df["saleable_type"].apply(lambda x: x.split("\\")[-1] if x is not None else None )
    final_ii_quantity = df.groupby(["sale_order_item_id"]).agg(
    final_ii_quantity = ("ii_quantity", "sum")
    ).final_ii_quantity
    df["final_ii_quantity"] = df.sale_order_item_id.apply(lambda x: final_ii_quantity[x] )
    is_both_invoiced = df.groupby("sale_order_id").agg(
    is_both = ("invoice_id", "nunique")
    ).is_both
    df["is_both_invoiced"] =  df["sale_order_id"].apply(lambda x: 0 if is_both_invoiced[x] > 1 else 0)
    is_invoiced = df.groupby(["sale_order_item_id"]).agg(
    is_invoiced = ("ii_quantity", "sum")
    ).is_invoiced
    df["is_invoiced"] = df.sale_order_item_id.apply(lambda x: 1 if final_ii_quantity[x] > 0 else 0 )
    df["is_partially_invoiced"] = df[["quantity", "final_ii_quantity", "is_invoiced"]].apply(lambda x: 1 if x["quantity"] > x["final_ii_quantity"] and x["is_invoiced"] == 1 else 0 , axis =1 )
    df["non_invoiced_quantity"] = df[["quantity", "final_ii_quantity"]].apply(lambda x: x["quantity"] - x["final_ii_quantity"] , axis =1 )
    for i in df.select_dtypes("int").columns:
        df[i] = df[i].astype(int).apply(lambda x: int(x) if x is not np.isnan(x) else None)
    for i in df.select_dtypes("float").columns:
        df[i] = df[i].astype(float).apply(lambda x: float(x) if x is not np.isnan(x) else None)
    df["sale_order_date"] = pd.to_datetime(df["sale_order_date"])
    df["invoice_date"] = pd.to_datetime(df["invoice_date"])
    df.billing_address = df.billing_address.apply(lambda x : eval(eval(x)))
    df["address_city"] = df.billing_address.apply(lambda x : x["city"] )
    df["address_pincode"] = df.billing_address.apply(lambda x : x["pincode"] )
    df["address_state"] = df.billing_address.apply(lambda x : x["state"] )
    df.drop(["billing_address"], axis=1, inplace=True)
    df = df.replace({np.nan: None})
    df = df[['sale_order_item_id', 'itemParentable_type', 'itemParentable_id',
            'saleable_type', 'saleable_id', 'sale_order_item_discount_percentage',
            'sale_order_item_mrp', 'unit_price_with_tax', 'unit_price_without_tax',
            'cgst_tax_rate', 'sgst_tax_rate', 'igst_tax_rate', 'cgst_tax_amount',
            'sgst_tax_amount', 'igst_tax_amount', 'is_dc', 'sale_order_id',
            'sale_order_date', 'sale_order_net_total', 'sales_channel',
            'order_state', 'title_name', 'edition_name', 'ref_code',
            'master_title_type', 'master_title_sub_type', 'meta_data',
            'invoice_item_id', 'quantity', 'ii_quantity','final_ii_quantity', 'is_both_invoiced', 'is_partially_invoiced',
            'non_invoiced_quantity', 'invoice_item_is_bill', 'invoice_item_is_dc',
            'is_invoiced', 'invoice_id', 'invoice_prefix', 'invoice_is_bill',
            'invoice_cr_dr_state', 'invoice_state', 'invoice_round_off',
            'invoice_date', 'invoice_total_with_tax',
            'total_unit_price_without_tax', 'advance_amount', 'cash_amount', 'cheque_amount', 'online_amount', 'contactable_type',
            'sales_group_id', 'sales_group_name', 'rsm',
            'contactable_id', 'customer_name', 'customer_category', 'branch_id', 'branch_name','address_city', 'address_pincode', 'address_state', 
            'packed_2_delivered_tat_day', 'create_2_processing_tat_day', 'processing_2_complete_tat_day', 'fetch_date']]
    df = df.applymap(lambda x: x.item() if isinstance(x, np.generic) else x)
    return df

def load_data(df, connection):
    mycursor = connection.cursor()
    load_chunk_size = int(conf.get("app", "load_chunk_size"))
    row_count = df.shape[0]
    load_counter = 1
    for i in df.index:
        row = df.iloc[i]
        row = row.apply(lambda x: x.item() if isinstance(x, np.generic) else x)
        row = row.replace({np.nan: None})
        query = f"""insert into {table} (sale_order_item_id,itemParentable_type,itemParentable_id,saleable_type,saleable_id,sale_order_item_discount_percentage,sale_order_item_mrp,unit_price_with_tax,unit_price_without_tax,cgst_tax_rate,sgst_tax_rate,igst_tax_rate,cgst_tax_amount,sgst_tax_amount,igst_tax_amount,is_dc,sale_order_id,sale_order_date,sale_order_net_total,sales_channel,order_state,title_name,edition_name,ref_code,master_title_type,master_title_sub_type,meta_data,invoice_item_id,quantity,ii_quantity,final_ii_quantity,is_both_invoiced,is_partially_invoiced,non_invoiced_quantity,invoice_item_is_bill,invoice_item_is_dc,is_invoiced,invoice_id,invoice_prefix,invoice_is_bill,invoice_cr_dr_state,invoice_state,invoice_round_off,invoice_date,invoice_total_with_tax,total_unit_price_without_tax,advance_amount,cash_amount,cheque_amount,online_amount,contactable_type,sales_group_id,sales_group_name,rsm,contactable_id,customer_name,customer_category,branch_id,branch_name,address_city,address_pincode,address_state,packed_2_delivered_tat_day,create_2_processing_tat_day,processing_2_complete_tat_day,fetch_date) 
            values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """
        mycursor.execute(query, 
            (
                row['sale_order_item_id'], row['itemParentable_type'], row['itemParentable_id'], row['saleable_type'], row['saleable_id'], row['sale_order_item_discount_percentage'], row['sale_order_item_mrp'], row['unit_price_with_tax'], row['unit_price_without_tax'], row['cgst_tax_rate'], row['sgst_tax_rate'], row['igst_tax_rate'], row['cgst_tax_amount'], row['sgst_tax_amount'], row['igst_tax_amount'], row['is_dc'], row['sale_order_id'], row['sale_order_date'], row['sale_order_net_total'], row['sales_channel'], row['order_state'], row['title_name'], row['edition_name'], row['ref_code'], row['master_title_type'], row['master_title_sub_type'], row['meta_data'], row['invoice_item_id'], row['quantity'], row['ii_quantity'], row['final_ii_quantity'], row['is_both_invoiced'], row['is_partially_invoiced'], row['non_invoiced_quantity'], row['invoice_item_is_bill'], row['invoice_item_is_dc'], row['is_invoiced'], row['invoice_id'], row['invoice_prefix'], row['invoice_is_bill'], row['invoice_cr_dr_state'], row['invoice_state'], row['invoice_round_off'], row['invoice_date'], row['invoice_total_with_tax'], row['total_unit_price_without_tax'], row['advance_amount'], row['cash_amount'], row['cheque_amount'], row['online_amount'], row['contactable_type'], row['sales_group_id'], row['sales_group_name'], row['rsm'], row['contactable_id'], row['customer_name'], row['customer_category'], row['branch_id'], row['branch_name'], row['address_city'], row['address_pincode'], row['address_state'], row['packed_2_delivered_tat_day'], row['create_2_processing_tat_day'], row['processing_2_complete_tat_day'], row['fetch_date']
            )
        )
    #     if load_counter >= load_chunk_size :
    #         connection.commit()
    #         load_counter = 0
    #     load_counter += 1
    # if row_count % load_chunk_size != 0:
    #     connection.commit()

def processing(counter, chunk):
    start_time = time.time()
    logging.info(f"Chunk no: {counter} in process")
    conf.set("app", "last_chunk_in_processing", str(counter))
    with open("./configure.yaml", "w") as config_file:
        conf.write(config_file)

    ############################################################## Fetching 
    logging.info(f"Chunk no: {counter} data fetch started")
    try:
        source_connection = connection_pool_source.get_connection()
        fetched_data = fetch_chunk(chunk, source_connection)
    except Exception as e:
        logging.warn(f"Failed chunk fetching for {counter} chunk : {e}") 
        if 'source_connection' in locals():
            source_connection.rollback()
    finally:
        if 'source_connection' in locals():
            source_connection.close()
    logging.info(f"Chunk no: {counter} data fetch done")    

    ############################################################## Transformation 
    logging.info(f"Chunk no: {counter} transformation started")
    transformed_data = data_transform(fetched_data)
    logging.info(f"Chunk no: {counter} transformation done")

    ############################################################## Loading 
    logging.info(f"Chunk no: {counter} loading started")
    try:
        destination_connection = connection_pool_destination.get_connection()
        load_data(transformed_data, destination_connection)
        destination_connection.commit()
    except Exception as e:
        logging.warn(f"Failed Loading for {counter} chunk : {e}")
        if 'destination_connection' in locals():
            destination_connection.rollback()
        logging.info(f"Chunk No: {counter} Retrying")
        processing(counter, chunk)
    finally:
        if 'destination_connection' in locals():
            destination_connection.close()
    logging.info(f"Chunk no: {counter} loading done")
    end_time = time.time() 
    logging.info(f"Chunk no: {counter} completion time {end_time - start_time}")

def synch(date):
    source_connection = connection_pool_source.get_connection()
    synchs = ["created_at", "updated_at"]
    new_sale_orders = []
    updated_sale_orders = []
    for synch_type in synchs: 
        logging.info(f"{synch_type} Synch Started")
        s1 = f"""
            SELECT 
                DISTINCT(so.id) as sale_order_id
        
            /* Sale Order and Invoice Items Tables */
            FROM sale_order_items as soi 
            inner join sale_orders as so on so.id = soi.sale_order_id
            left join invoice_items as ii on ii.sale_order_item_id = soi.id
        
            /* Invoice Tables */
            LEFT JOIN invoices AS i ON ii.invoice_id = i.id
        
            /* Advance Amount */
            left join ( 
            SELECT 
                i.id AS invoice_id, SUM(cr_dr.amount) AS advance_amount,
                SUM( 
                    CASE
                        WHEN l1.{synch_type} > '{date}' or l2.{synch_type} > '{date}' or cr_dr.{synch_type} > '{date}' or p.{synch_type} > '{date}' THEN 1
                        ELSE 0
                    END
                ) as creation_updation_flag
            FROM invoices AS i 
            INNER JOIN ledger_transactions AS l1 ON l1.ledgerable_id = i.id AND l1.ledgerable_type LIKE "%Invoice" AND l1.state = "ACTIVE"
            INNER JOIN credit_debit_mappings AS cr_dr ON cr_dr.debit_id = l1.id
            INNER JOIN ledger_transactions AS l2 ON l2.id = cr_dr.credit_id AND l2.ledgerable_type LIKE "%Payment"  AND l2.state = "ACTIVE"
            INNER JOIN payments AS p ON p.id = l2.ledgerable_id
            WHERE p.cr_dr_state IN ("SETTLED", "PARTIAL_SETTLED") AND l1.transaction_datetime > l2.transaction_datetime
            GROUP by i.id ) as advance on advance.invoice_id = i.id
        
            /* Payment Mode */
            LEFT JOIN ( 
            SELECT 
                i.id AS invoice_id,
                SUM( CASE WHEN p.mode = "CASH" THEN cr_dr.amount ELSE 0 END ) as cash_amount,
                SUM( CASE WHEN p.mode = "CHEQUE" THEN cr_dr.amount ELSE 0 END ) as cheque_amount,
                SUM( CASE WHEN p.mode = "ONLINE" THEN cr_dr.amount ELSE 0 END ) as online_amount,
                SUM( 
                    CASE
                        WHEN l1.{synch_type} > '{date}' or l2.{synch_type} > '{date}' or cr_dr.{synch_type} > '{date}' or p.{synch_type} > '{date}' THEN 1
                        ELSE 0
                    END
                ) as creation_updation_flag
            FROM invoices AS i 
            INNER JOIN ledger_transactions AS l1 ON l1.ledgerable_id = i.id AND l1.ledgerable_type LIKE "%Invoice" AND l1.state = "ACTIVE" 
            INNER JOIN credit_debit_mappings AS cr_dr ON cr_dr.debit_id = l1.id 
            INNER JOIN ledger_transactions AS l2 ON l2.id = cr_dr.credit_id AND l2.ledgerable_type LIKE "%Payment" AND l2.state = "ACTIVE" 
            INNER JOIN payments AS p ON p.id = l2.ledgerable_id 
            WHERE p.cr_dr_state IN ("SETTLED", "PARTIAL_SETTLED")
            GROUP by i.id ) as p_mode on p_mode.invoice_id = i.id """
        
        s2 = f""" 
            /* TAT */
            /*Shipment TAT*/
            LEFT JOIN ( 
                SELECT 
                    s.sale_order_id, 
                    MIN(DATEDIFF(sl2.created_at, sl1.created_at)) AS packed_2_delivered_tat_day, 
                    MIN(TIMEDIFF(sl2.created_at, sl1.created_at)) AS packed_2_delivered_tat_minutes,
                    SUM(
                        CASE
                            WHEN s.{synch_type} > '{date}' or sl1.{synch_type} > '{date}' or sl2.{synch_type} > '{date}' THEN 1
                            ELSE 0
                        END
                    ) as creation_updation_flag
                FROM shipments AS s 
                LEFT JOIN shipment_state_logs AS sl1 ON sl1.shipment_id = s.id AND sl1.state LIKE "PACKED" 
                LEFT JOIN shipment_state_logs AS sl2 ON sl2.shipment_id = s.id AND sl2.state LIKE "DELIVERED" 
                WHERE s.created_at > "2022/10/01" 
                GROUP BY s.sale_order_id 
                HAVING packed_2_delivered_tat_day IS NOT NULL) AS s_tat on s_tat.sale_order_id = so.id
        
            /* Sale Order TAT */
            LEFT JOIN ( 
                SELECT 
                    so.id as sale_order_id, 
                    MIN(DATEDIFF(c2p2.created_at,c2p1.created_at)) as create_2_processing_tat_day, 
                    MIN(TIMEDIFF(c2p2.created_at, c2p1.created_at)) as create_2_processing_tat_minutes, 
                    MIN(DATEDIFF(p2c2.created_at,p2c1.created_at)) as processing_2_complete_tat_day, 
                    MIN(TIMEDIFF(p2c2.created_at, p2c1.created_at)) as processing_2_complete_tat_minutes,
                    SUM(
                        CASE
                            WHEN c2p1.{synch_type} > '{date}' or c2p2.{synch_type} > '{date}' or p2c1.{synch_type} > '{date}' or p2c2.{synch_type} > '{date}' THEN 1
                            ELSE 0
                        END
                    ) as creation_updation_flag
                FROM sale_orders as so 
                left join sale_order_state_logs as c2p1 on c2p1.sale_order_id = so.id and c2p1.state in ("CREATED")
                left join sale_order_state_logs as c2p2 on c2p2.sale_order_id = so.id and c2p2.state in ("SEND_FOR_DIGITAL", "PROCESSED")
                left join sale_order_state_logs as p2c1 on p2c1.sale_order_id = so.id and p2c1.state in ("SEND_FOR_DIGITAL", "PROCESSED")
                left join sale_order_state_logs as p2c2 on p2c2.sale_order_id = so.id and p2c2.state in ("COMPLETED") 
                where so.state in ("COMPLETED", "FORCED_COMPLETED") and so.created_at > "2022/10/01"
                group by so.id
                HAVING create_2_processing_tat_day is not null ) as so_tat on so_tat.sale_order_id = so.id """
        
        s3 = f""" 
            /* Product Tables */
            LEFT JOIN edition_product_types AS ept on ept.id = soi.saleable_id
            LEFT JOIN editions AS e ON e.id = ept.edition_id
            LEFT JOIN titles as t on t.id = e.title_id
            LEFT JOIN master_title_types as mtt on mtt.id = t.master_title_type_id
            LEFT JOIN master_title_sub_types as mtst on mtst.id = t.master_title_sub_type_id
            LEFT JOIN standard_meta_data as smd on smd.id = soi.itemParentable_id AND soi.itemParentable_type LIKE "%StandardMetaData" AND soi.saleable_type like "%EditionProductType"
            LEFT JOIN notebook_title_details as ntd on ntd.id = soi.itemParentable_id and soi.itemParentable_type LIKE "%NotebookTitleDetail"
        
            /* RSM Tables */
            LEFT JOIN branches as b on so.branch_id = b.id
            LEFT JOIN sales_groups as sg on b.sales_group_id = sg.id
            LEFT JOIN employees as emp on sg.rsm = emp.id
            LEFT JOIN contact_individual_details as cid on cid.contact_id = emp.contact_id
        
            /* Customer Tables */
            LEFT JOIN customers as c on c.id = so.contactable_id AND so.contactable_type LIKE "%Customer" 
        
            /* Filters Tables */
            WHERE ( soi.{synch_type} > '{date}' or so.{synch_type} > '{date}' or ii.{synch_type} > '{date}' or i.{synch_type} > '{date}' """
        s4 = f"""
                or so_tat.creation_updation_flag > 1 or s_tat.creation_updation_flag > 1
            """
        s5 = f"""
                or advance.creation_updation_flag > 0 or p_mode.creation_updation_flag > 0) and so.created_at > '{date}'
                and ( soi.saleable_type LIKE "%EditionProductType"  ) AND ( so.state in ("PARTIAL_COMPLETED", "COMPLETED", "FORCED_COMPLETED", "CANCELLED")) 
            """
        
        if synch_type == "created_at":
            q = s1 + s2 + s3 + s4 + s5
            new_sale_orders = list( pd.read_sql_query(q, source_connection).sale_order_id.values )
        else:
            q = s1 + s3 + s5
            updated_sale_orders = list( pd.read_sql_query(q, source_connection).sale_order_id.values )
        logging.info(f"{synch_type} Synch Done")
    source_connection.close()
    return set(sorted(list(set(tuple(new_sale_orders) + tuple(updated_sale_orders)))))
@dag(
    dag_id= "Regular_orders_threading_prod"
)

def test():
    @task
    def synch_data():
        logging.info("Synch Started")
        date = conf_state.get("db_state", "last_synch_date")
        synch_ids = synch(date)
        chunk_size = int(conf.get("app", "chunk_size"))
        id_count = len(synch_ids)
        logging.info(f"Synch End | total {id_count} ids found with {math.ceil(id_count/chunk_size)} chunks")
        return synch_ids
    
    @task
    def deletion(ids):
            logging.info(f"deletion started")
            try:
                destination_connection = connection_pool_destination.get_connection()
                delete_chunk(tuple(ids), destination_connection)
                destination_connection.commit()
            except Exception as e:
                logging.warn(f"Failed Deletion : {e}")
                if 'destination_connection' in locals():
                    destination_connection.rollback()
            finally:
                if 'destination_connection' in locals():
                    destination_connection.close()
            logging.info(f"Deletion done")
            return ids

    @task
    def e_t_l(ids):
        logging.info("Chunking Started")
        ids = tuple(ids)
        chunk_size = int(conf.get("app", "chunk_size"))
        counter = 1
        logging.info(f"Total {len(ids) / chunk_size} chunks found")
        future_to_chunk = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=THREAD_COUNT) as executor:
            for i in range(0, len(ids), chunk_size):
                chunk = ids[i:i+chunk_size]
                future = executor.submit(processing, counter, chunk)
                future_to_chunk[future] = chunk
                counter += 1
        return True

    @task
    def save_state(state):
        if state == True:
            conf_state.set("db_state", "last_synch_date", str(datetime.now()))
            with open("./db_state.yaml", "w") as config_file:
                conf_state.write(config_file)
        return True

    synched_ids = synch_data()
    deleted_ids = deletion(synched_ids)
    state = e_t_l(deleted_ids)
    save_state(state)

test()
