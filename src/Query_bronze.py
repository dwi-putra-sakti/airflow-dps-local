import time
import pandas as pd
import numpy as np
import sys
import json
import pytz
from rest_framework.response import Response
from rest_framework import status
from rest_framework.exceptions import ValidationError
from core.db import DatabaseConnector
from datetime import datetime, timedelta
dbLoad = 'default'
dbPRoll = 'payroll'
dbSave = 'local'
dbPRS='payroll_live'
def parse_sql_params(colom="*", distinct=False, order_by="", limit=None, **filters):
    distinct_clause = "DISTINCT" if distinct else ""
    where_conditions = ["1=1"]

    for key, value in filters.items():
        if value is None:
            continue

        operator = "="
        clean_key = key

        # Parsing suffix operator
        if key.endswith("__is_not_null") or key.endswith("__not_null"):
            clean_key = key.replace("__is_not_null", "").replace("__not_null", "")
            operator = "IS NOT NULL"
        elif key.endswith("__isnull") or key.endswith("__is_null"):
            clean_key = key.replace("__isnull", "").replace("__is_null", "")
            operator = "IS NULL" if bool(value) else "IS NOT NULL"
        elif key.endswith("__between"):
            clean_key = key.replace("__between", "")
            operator = "BETWEEN"
        elif key.endswith("__not_in"):
            clean_key = key.replace("__not_in", "")
            operator = "NOT IN"
        elif key.endswith("__like"):
            clean_key = key.replace("__like", "")
            operator = "LIKE"
        elif key.endswith("__in"):
            clean_key = key.replace("__in", "")
            operator = "IN"
        elif key.endswith("__not") or key.endswith("__neq"):
            clean_key = key.replace("__not", "").replace("__neq", "")
            operator = "<>"
        elif key.endswith("__gt"):
            clean_key = key.replace("__gt", "")
            operator = ">"
        elif key.endswith("__gte"):
            clean_key = key.replace("__gte", "")
            operator = ">="
        elif key.endswith("__lt"):
            clean_key = key.replace("__lt", "")
            operator = "<"
        elif key.endswith("__lte"):
            clean_key = key.replace("__lte", "")
            operator = "<="

        # Replace double underscore '__' dengan titik '.'
        clean_key = clean_key.replace("__", ".")
        # 2. Format Value
        if operator in ["IS NULL", "IS NOT NULL"]:
            # Khusus NULL check, tidak perlu mencantumkan value angka/string di SQL
            where_conditions.append(f"{clean_key} {operator}")
        elif operator == "BETWEEN":
            if isinstance(value, (list, tuple, set)) and len(value) == 2:
                v1 = f"'{value[0]}'" if isinstance(value[0], str) else str(value[0])
                v2 = f"'{value[1]}'" if isinstance(value[1], str) else str(value[1])
                where_conditions.append(f"{clean_key} BETWEEN {v1} AND {v2}")
            else:
                # Abaikan jika format value untuk BETWEEN tidak valid
                continue
        elif isinstance(value, (list, tuple)):
            if not value:
                continue
            formatted_values = ", ".join([f"'{v}'" if isinstance(v, str) else str(v) for v in value])
            op_list = operator if operator in ["IN", "NOT IN"] else "IN"
            where_conditions.append(f"{clean_key} {op_list} ({formatted_values})")
        elif isinstance(value, str):
            where_conditions.append(f"{clean_key} {operator} '{value}'")
        else:
            where_conditions.append(f"{clean_key} {operator} {value}")

    # PASTI KAN ADA SPASI / KONSISTENSI KLAUSA
    where_clause = " WHERE " + " AND ".join(where_conditions) if where_conditions else ""
    order_clause = f" ORDER BY {order_by}" if order_by else ""
    limit_clause = f" LIMIT {limit}" if limit else ""

    return distinct_clause, colom, where_clause, order_clause, limit_clause

class DataLayerMixin:
    # one layer
    def df_cust_(self, **kwargs):
        return one_layer.customers_pb__gloc__df(**kwargs)
    def df_prov_(self, **kwargs):
        return one_layer.ref_provinsi__gloc__df(**kwargs)
    def df_kabkot_(self, **kwargs):
        return one_layer.ref_kabkota__gloc__df(**kwargs)
    def df_kec_(self, **kwargs):
        return one_layer.ref_kecamatan__gloc__df(**kwargs)
    def df_keldes_(self, **kwargs):
        return one_layer.ref_kelurahan__gloc__df(**kwargs)
    def df_empspg_(self, **kwargs):
        return one_layer.employeesspg__gloc__df(**kwargs)
    def df_cus_stay_(self, **kwargs):
        return one_layer.m_cus_stay__gproll__df(**kwargs)
    def df_approv_(self, **kwargs):
        return one_layer.appropval__gproll__df(**kwargs)
    def df_employee_sl(self, **kwargs):
        return one_layer.employees_tb__glive__df(**kwargs)
    def df_user_(self, **kwargs):
        return one_layer.user_tb__glive__df(**kwargs)
    def df_bank_(self, **kwargs):
        return one_layer.t_bank__glive__df(**kwargs)
    def df_province_(self, **kwargs):
        return one_layer.province_tb__glive__df(**kwargs)
    def df_ptkp_(self, **kwargs):
        return one_layer.tb_ptkp__glive__df(**kwargs)
    def df_userlevel_(self, **kwargs):
        return one_layer.t_userlevel__glive__df(**kwargs)
    def df_city_(self, **kwargs):
        return one_layer.city_tb__glive__df(**kwargs)
    def df_custb(self, **kwargs):
        return one_layer.customers_tb__gloc__df(**kwargs)
    # Multi layer
    def df_alamat(self, **kwargs):
        return multi_layer.alamat__gloc__df(**kwargs)
    def df_toko_handle_(self, **kwargs):
        return multi_layer.toko_hendle__glive__df(**kwargs)
    def df_user_lv_(self, **kwargs):
        return multi_layer.user_level__glive__df(**kwargs)
    def df_approv_ml(self, **kwargs):
        return multi_layer.appropval__gproll__df(**kwargs)
    def df_employee_(self, **kwargs):
        return multi_layer.employees_tb__glive__df(**kwargs)
    
class one_layer():
    @staticmethod
    def customers_pb__gloc__df(colom="*", distinct=False,  order_by="", limit=None, df=True, queri=False, **filters):
        # 1. Ekstrak komponen SQL dari parameter
        distinct_cl, cols, where_cl, order_cl, limit_cl = parse_sql_params(
            colom=colom, distinct=distinct, order_by=order_by, limit=limit, **filters)
        # 2. Rakit Query
        query = f"""SELECT {distinct_cl} {cols} FROM customers_pb {where_cl}{order_cl}{limit_cl}"""
        if queri:
            print("--- RUNNING QUERY ---")
            print(query)
        # 3. Eksekusi
        results = DatabaseConnector.execute_raw_sql(query, using=dbLoad)
        if df==True:return pd.DataFrame(results)
        return results
    
    @staticmethod
    def ref_provinsi__gloc__df(colom="*", distinct=False,  order_by="", limit=None, df=True, queri=False, **filters):
        # 1. Ekstrak komponen SQL dari parameter
        distinct_cl, cols, where_cl, order_cl, limit_cl = parse_sql_params(
            colom=colom, distinct=distinct, order_by=order_by, limit=limit, **filters)
        # 2. Rakit Query
        query = f"""SELECT {distinct_cl} {cols} FROM ref_provinsi {where_cl}{order_cl}{limit_cl}"""
        if queri:
            print("--- RUNNING QUERY ---")
            print(query)
        # 3. Eksekusi
        results = DatabaseConnector.execute_raw_sql(query, using=dbLoad)
        if df==True:return pd.DataFrame(results)
        return results
    
    @staticmethod
    def ref_kabkota__gloc__df(colom="*", distinct=False,  order_by="", limit=None, df=True, queri=False, **filters):
        # 1. Ekstrak komponen SQL dari parameter
        distinct_cl, cols, where_cl, order_cl, limit_cl = parse_sql_params(
            colom=colom, distinct=distinct, order_by=order_by, limit=limit, **filters)
        # 2. Rakit Query
        query = f"""SELECT {distinct_cl} {cols} FROM ref_kabkota {where_cl}{order_cl}{limit_cl}"""
        if queri:
            print("--- RUNNING QUERY ---")
            print(query)
        # 3. Eksekusi
        results = DatabaseConnector.execute_raw_sql(query, using=dbLoad)
        if df==True:return pd.DataFrame(results)
        return results
    
    @staticmethod
    def ref_kecamatan__gloc__df(colom="*", distinct=False,  order_by="", limit=None, df=True, queri=False, **filters):
        # 1. Ekstrak komponen SQL dari parameter
        distinct_cl, cols, where_cl, order_cl, limit_cl = parse_sql_params(
            colom=colom, distinct=distinct, order_by=order_by, limit=limit, **filters)
        # 2. Rakit Query
        query = f"""SELECT {distinct_cl} {cols} FROM ref_kecamatan {where_cl}{order_cl}{limit_cl}"""
        if queri:
            print("--- RUNNING QUERY ---")
            print(query)
        # 3. Eksekusi
        results = DatabaseConnector.execute_raw_sql(query, using=dbLoad)
        if df==True:return pd.DataFrame(results)
        return results
    
    @staticmethod
    def ref_kelurahan__gloc__df(colom="*", distinct=False,  order_by="", limit=None, df=True, queri=False, **filters):
        # 1. Ekstrak komponen SQL dari parameter
        distinct_cl, cols, where_cl, order_cl, limit_cl = parse_sql_params(
            colom=colom, distinct=distinct, order_by=order_by, limit=limit, **filters)
        # 2. Rakit Query
        query = f"""SELECT {distinct_cl} {cols} FROM ref_kelurahan {where_cl}{order_cl}{limit_cl}"""
        if queri:
            print("--- RUNNING QUERY ---")
            print(query)
        # 3. Eksekusi
        results = DatabaseConnector.execute_raw_sql(query, using=dbLoad)
        if df==True:return pd.DataFrame(results)
        return results
    
    @staticmethod
    def employeesspg__gloc__df(colom="*", distinct=False,  order_by="", limit=None, df=True, queri=False, **filters):
        # 1. Ekstrak komponen SQL dari parameter
        distinct_cl, cols, where_cl, order_cl, limit_cl = parse_sql_params(
            colom=colom, distinct=distinct, order_by=order_by, limit=limit, **filters)
        # 2. Rakit Query
        query = f"""SELECT {distinct_cl} {cols} FROM employeesspg_tb {where_cl}{order_cl}{limit_cl}"""
        if queri:
            print("--- RUNNING QUERY ---")
            print(query)
        # 3. Eksekusi
        results = DatabaseConnector.execute_raw_sql(query, using=dbLoad)
        if df==True:return pd.DataFrame(results)
        return results
    
    @staticmethod
    def m_cus_stay__gproll__df(colom="*", distinct=False,order_by="", limit=None,df=True, queri=False, **filters):
        # 1. Ekstrak komponen SQL dari parameter
        distinct_cl, cols, where_cl, order_cl, limit_cl = parse_sql_params(
            colom=colom, distinct=distinct, order_by=order_by, limit=limit, **filters)
        # 2. Rakit Query
        query = f"""SELECT {distinct_cl} {cols} FROM m_cus_stay {where_cl}{order_cl}{limit_cl}
        """
        if queri:
            print("--- RUNNING QUERY ---")
            print(query)
        # 3. Eksekusi
        results = DatabaseConnector.execute_raw_sql(query, using=dbPRoll)
        if df==True:return pd.DataFrame(results)
        return results
    
    @staticmethod
    def appropval__gproll__df(colom="*", distinct=False, order_by="", limit=None,df=True, queri=False, **filters):
        # 1. Ekstrak komponen SQL dari parameter
        distinct_cl, cols, where_cl, order_cl, limit_cl = parse_sql_params(
            colom=colom, distinct=distinct, order_by=order_by, limit=limit, **filters)
        # 2. Rakit Query
        query = f"""SELECT {distinct_cl} {cols} FROM approval_dps_dev {where_cl}{order_cl}{limit_cl}
        """
        if queri:
            print("--- RUNNING QUERY ---")
            print(query)
        # 3. Eksekusi
        results = DatabaseConnector.execute_raw_sql(query, using=dbPRoll)
        if df==True:return pd.DataFrame(results)
        return results
    
    @staticmethod
    def employees_tb__glive__df(colom="*", distinct=False, order_by="", limit=None, df=True, queri=False, **filters):
        # 1. Ekstrak komponen SQL dari parameter
        distinct_cl, cols, where_cl, order_cl, limit_cl = parse_sql_params(
            colom=colom, distinct=distinct, order_by=order_by, limit=limit, **filters)
        # 2. Rakit Query
        query = f""" SELECT {distinct_cl} {cols} FROM employees_tb emp {where_cl}{order_cl}{limit_cl}
        """
        if queri:
            print("--- RUNNING QUERY ---")
            print(query)
        # 3. Eksekusi
        results = DatabaseConnector.execute_raw_sql(query, using=dbSave)
        if df==True:return pd.DataFrame(results)
        return pd.DataFrame(results) 

    @staticmethod
    def user_tb__glive__df(colom="*", distinct=False, order_by="", limit=None, df=True, queri=False, **filters):
        # 1. Ekstrak komponen SQL dari parameter
        distinct_cl, cols, where_cl, order_cl, limit_cl = parse_sql_params(
            colom=colom, distinct=distinct, order_by=order_by, limit=limit, **filters )
        # 2. Rakit Query
        query = f"""SELECT {distinct_cl} {cols} FROM user_tb {where_cl}{order_cl}{limit_cl}
        """
        if queri:
            print("--- RUNNING QUERY ---")
            print(query)
        # 3. Eksekusi
        results = DatabaseConnector.execute_raw_sql(query, using=dbSave)
        if df==True:return pd.DataFrame(results)
        return pd.DataFrame(results) 
    
    @staticmethod
    def t_bank__glive__df(colom="*", distinct=False, order_by="", limit=None, df=True, queri=False, **filters):
        # 1. Ekstrak komponen SQL dari parameter
        distinct_cl, cols, where_cl, order_cl, limit_cl = parse_sql_params(
            colom=colom, distinct=distinct, order_by=order_by, limit=limit, **filters )
        # 2. Rakit Query
        query = f"""SELECT {distinct_cl} {cols} FROM t_bank {where_cl}{order_cl}{limit_cl}
        """
        if queri:
            print("--- RUNNING QUERY ---")
            print(query)
        # 3. Eksekusi
        results = DatabaseConnector.execute_raw_sql(query, using=dbPRS)
        if df==True:return pd.DataFrame(results)
        return pd.DataFrame(results) 
    
    @staticmethod
    def province_tb__glive__df(colom="*", distinct=False, order_by="", limit=None, df=True, queri=False, **filters):
        # 1. Ekstrak komponen SQL dari parameter
        distinct_cl, cols, where_cl, order_cl, limit_cl = parse_sql_params(
            colom=colom, distinct=distinct, order_by=order_by, limit=limit, **filters )
        # 2. Rakit Query
        query = f"""SELECT {distinct_cl} {cols} FROM province_tb {where_cl}{order_cl}{limit_cl}
        """
        if queri:
            print("--- RUNNING QUERY ---")
            print(query)
        # 3. Eksekusi
        results = DatabaseConnector.execute_raw_sql(query, using=dbPRS)
        if df==True:return pd.DataFrame(results)
        return pd.DataFrame(results) 

    @staticmethod
    def city_tb__glive__df(colom="*", distinct=False, order_by="", limit=None, df=True, queri=False, **filters):
        # 1. Ekstrak komponen SQL dari parameter
        distinct_cl, cols, where_cl, order_cl, limit_cl = parse_sql_params(
            colom=colom, distinct=distinct, order_by=order_by, limit=limit, **filters )
        # 2. Rakit Query
        query = f"""SELECT {distinct_cl} {cols} FROM city_tb {where_cl}{order_cl}{limit_cl}
        """
        if queri:
            print("--- RUNNING QUERY ---")
            print(query)
        # 3. Eksekusi
        results = DatabaseConnector.execute_raw_sql(query, using=dbPRS)
        if df==True:return pd.DataFrame(results)
        return pd.DataFrame(results) 
    
    @staticmethod
    def tb_ptkp__glive__df(colom="*", distinct=False, order_by="", limit=None, df=True, queri=False, **filters):
        # 1. Ekstrak komponen SQL dari parameter
        distinct_cl, cols, where_cl, order_cl, limit_cl = parse_sql_params(
            colom=colom, distinct=distinct, order_by=order_by, limit=limit, **filters )
        # 2. Rakit Query
        query = f"""SELECT {distinct_cl} {cols} FROM tb_ptkp {where_cl}{order_cl}{limit_cl}
        """
        if queri:
            print("--- RUNNING QUERY ---")
            print(query)
        # 3. Eksekusi
        results = DatabaseConnector.execute_raw_sql(query, using=dbPRS)
        if df==True:return pd.DataFrame(results)
        return pd.DataFrame(results) 
    
    @staticmethod
    def t_userlevel__glive__df(colom="*", distinct=False, order_by="", limit=None, df=True, queri=False, **filters):
        # 1. Ekstrak komponen SQL dari parameter
        distinct_cl, cols, where_cl, order_cl, limit_cl = parse_sql_params(
            colom=colom, distinct=distinct, order_by=order_by, limit=limit, **filters )
        # 2. Rakit Query
        query = f"""SELECT {distinct_cl} {cols} FROM t_userlevel {where_cl}{order_cl}{limit_cl}
        """
        if queri:
            print("--- RUNNING QUERY ---")
            print(query)
        # 3. Eksekusi
        results = DatabaseConnector.execute_raw_sql(query, using=dbPRS)
        if df==True:return pd.DataFrame(results)
        return pd.DataFrame(results) 
    @staticmethod
    def customers_tb__gloc__df(colom="*", distinct=False,  order_by="", limit=None, df=True, queri=False, **filters):
        # 1. Ekstrak komponen SQL dari parameter
        distinct_cl, cols, where_cl, order_cl, limit_cl = parse_sql_params(
            colom=colom, distinct=distinct, order_by=order_by, limit=limit, **filters)
        # 2. Rakit Query
        query = f"""SELECT {distinct_cl} {cols} FROM customer_tb {where_cl}{order_cl}{limit_cl}"""
        if queri:
            print("--- RUNNING QUERY ---")
            print(query)
        # 3. Eksekusi
        results = DatabaseConnector.execute_raw_sql(query, using=dbSave)
        if df==True:return pd.DataFrame(results)
        return results
    
class multi_layer():
    @staticmethod
    def alamat__gloc__df(colom="*", distinct=False, 
                                order_by="", limit=None,df=True, queri=False, **filters):
        # 1. Ekstrak komponen SQL dari parameter
        distinct_cl, cols, where_cl, order_cl, limit_cl = parse_sql_params(
            colom=colom, distinct=distinct, order_by=order_by, limit=limit, **filters
        )
        # 2. Rakit Query
        query = f"""
            SELECT {distinct_cl} {cols}
            FROM ref_kelurahan keldes
            left join ref_kecamatan kec on keldes.fv_kec_id=kec.fv_kec_id
            left join ref_kabkota kabkot on kec.fv_kabkota_id=kabkot.fv_kabkota_id
            left join ref_provinsi prov on kabkot.fn_provid=prov.fn_provid
            {where_cl}{order_cl}{limit_cl}
        """
        if queri:
            print("--- RUNNING QUERY ---")
            print(query)
        
        # 3. Eksekusi
        results = DatabaseConnector.execute_raw_sql(query, using=dbLoad)
        if df==True:
            return pd.DataFrame(results)
        return results
        
    @staticmethod
    def appropval__gproll__df(colom="*", distinct=False, 
                               order_by="", limit=None,df=True, queri=False, **filters):
        # 1. Ekstrak komponen SQL dari parameter
        distinct_cl, cols, where_cl, order_cl, limit_cl = parse_sql_params(
            colom=colom, distinct=distinct, order_by=order_by, limit=limit, **filters
        )
        # 2. Rakit Query
        query = f"""
            SELECT {distinct_cl} {cols}
            FROM approval_dps_dev appr
            left join m_cus_stay c on appr.fn_userid=c.fn_userid
            {where_cl}{order_cl}{limit_cl}
        """
        if queri:
            print("--- RUNNING QUERY ---")
            print(query)
        
        # 3. Eksekusi
        results = DatabaseConnector.execute_raw_sql(query, using=dbPRoll)
        if df==True:
            return pd.DataFrame(results)
        return results
    
    @staticmethod
    def employees_tb__glive__df(colom="*", distinct=False, 
                               order_by="", limit=None,df=True, queri=False, **filters):
        # 1. Ekstrak komponen SQL dari parameter
        distinct_cl, cols, where_cl, order_cl, limit_cl = parse_sql_params(
            colom=colom, distinct=distinct, order_by=order_by, limit=limit, **filters
        )
        # 2. Rakit Query
        query = f"""
            SELECT {distinct_cl} {cols}
            FROM employees_tb emp
            left join  user_tb u on emp.fn_userid=u.fn_userid
            {where_cl}{order_cl}{limit_cl}
        """
        if queri:
            print("--- RUNNING QUERY ---")
            print(query)

        # 3. Eksekusi
        results = DatabaseConnector.execute_raw_sql(query, using=dbSave)
        if df==True:
            return pd.DataFrame(results)
        return results

    @staticmethod    
    def toko_hendle__glive__df(
        colom="u.fn_userid, IFNULL(c.fn_cusid, 0) AS fn_cusid, e.fv_nameemployees AS fv_nama, ts.fn_userid user_spv, ets.fv_nameemployees spv",
        distinct=True,
        order_by="c.fn_cusid",
        limit=None,
        df=True,
        queri=False,
        use_default_filters=True,  # Control toggle untuk default filter
        **filters
    ):
        """
        Helper untuk mengambil data toko/customer yang di-handle oleh user.
        `use_default_filters=True` akan menggunakan filter bawaan. 
        Set ke `False` jika ingin query tanpa default filter (murni dari **filters).
        """
        # 1. Tentukan Default Filters jika use_default_filters diset True
        if use_default_filters:
            base_filters = {
                "u__fn_userlevel__in": [1, 8, 13],
                "e__fn_iddepart__in": [8, 20],
                "e__fc_hold": "F",
                "u__fc_hold": "F",
                "c__fc_hold": "F"
            }
        else:
            base_filters = {}

        # 2. Gabungkan Base Filters dengan Filters Tambahan dari user
        final_filters = {**base_filters, **filters}

        # 3. Parse semua komponen query menggunakan parse_sql_params
        distinct_cl, cols, where_cl, order_cl, limit_cl = parse_sql_params(
            colom=colom, distinct=distinct, order_by=order_by, limit=limit, **final_filters
        )

        # 4. Rakit Query SQL
        query = f"""
            SELECT {distinct_cl} {cols}
            FROM user_tb u
            JOIN employees_tb e ON u.fn_userid = e.fn_userid
            JOIN t_aksesusercust t ON u.fn_userid = t.fn_userid
            JOIN customer_tb c ON t.fn_cusid = c.fn_cusid
            JOIN t_aksesusercust ts ON c.fn_cusid = ts.fn_cusid
            JOIN employees_tb ets ON ts.fn_userid = ets.fn_userid
            {where_cl}{order_cl}{limit_cl}
        """

        # 5. Print query jika mode debugging
        if queri:
            print("--- RUNNING QUERY ---")
            print(query)

        # 6. Eksekusi Query
        results = DatabaseConnector.execute_raw_sql(query, using=dbSave)
        if df:
            return pd.DataFrame(results)
        return results
    
    @staticmethod
    def user_level__glive__df(colom="*", distinct=False, 
                               order_by="", limit=None, queri=False, **filters):
        # 1. Ekstrak komponen SQL dari parameter
        distinct_cl, cols, where_cl, order_cl, limit_cl = parse_sql_params(
            colom=colom, distinct=distinct, order_by=order_by, limit=limit, **filters
        )
        # 2. Rakit Query
        query = f"""
            SELECT {distinct_cl} {cols}
            FROM user_tb u
            left join t_userlevel ul on u.fn_userlevel=ul.fn_userlevel
            {where_cl}{order_cl}{limit_cl}
        """
        if queri:
            print("--- RUNNING QUERY ---")
            print(query)

        # 3. Eksekusi
        results = DatabaseConnector.execute_raw_sql(query, using=dbSave)
        return pd.DataFrame(results) 