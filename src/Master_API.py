import traceback
import re
import sys
import pandas as pd
import numpy as np

from rest_framework.views import APIView
from django.http import JsonResponse, HttpResponseBadRequest, HttpResponse
from services.get_utils_API import (list_to_sql_in)
from services.Query_bronze import DataLayerMixin

def _catch_error_details(exception_obj):
        """Helper internal untuk melacak file, fungsi, dan baris tempat error terjadi"""
        exc_type, _, exc_tb = sys.exc_info()
        tb_details = traceback.extract_tb(exc_tb)[-1]
        # 1. Ambil traceback bawaan python
        full_traceback = traceback.format_exc()
        
        # 2. Pola untuk mencari 'views.py", line <angka>' tanpa peduli besar/kecil huruf
        #    \d+ artinya mencari kecocokan angka berapa pun (nomor baris)
        pola = r'(Master_API\.py", line \d+)'
        
        # 3. Masukkan enter baru (\n) sebelum dan sesudah pola tersebut, sekaligus buat CAPS LOCK agar mencolok
        full_traceback_formatted = re.sub(
            pola, 
            r'\n\n \1 <<<<<<<<<<<<<<\n', 
            full_traceback, 
            flags=re.IGNORECASE
        )
        return JsonResponse({
            'success': False, 
            'message': f"{str(exception_obj)} (Terjadi di {tb_details.filename}, baris {tb_details.lineno}, fungsi '{tb_details.name}')",
            'error_detail': {
                'file': tb_details.filename,
                'baris': tb_details.lineno,
                'fungsi': tb_details.name,
                'tipe_error': exc_type.__name__,
                'full_traceback': full_traceback_formatted
            }
        }, status=500)

class MasterView(DataLayerMixin,APIView):
    def post(self, request, *args, **kwargs):
        type_param = request.data.get('type')   
        print(type_param)
        if not type_param:
            return JsonResponse({
                'success': False, 
                'message': "Parameter 'type' wajib diisi."
            }, status=400)
        # Mapping 'type' parameter ke fungsi handler-nya
        routes = {
            'cust_loc': self.get_cust_loc,
            'alamat_loc': self.get_alamat,
            'a_prov_loc': self.get_a_prov,
            'a_kabkot_loc': self.get_a_kabkot,
            'a_kec_loc': self.get_a_kec,
            'a_keldes_loc': self.get_a_keldes,
            'employee_live': self.get_employee_live,
            'akses_user_cus_employee_live': self.get_akses_user_cus_employee_live,
            'bank_live': self.get_bank,
            'ptkp_live': self.get_ptkp,
            'provinsi_live': self.get_provinsi,
            'kota_live': self.get_kota,
            'userlevel_live': self.get_userlevel,
            'prov_kota_live': self.get_prov_kota,
        }
        handler = routes.get(type_param)
        if not handler:
            return JsonResponse({
                'success': False, 
                'message': f"Method type '{type_param}' tidak valid atau tidak ditemukan."
            }, status=400)
        try:
            # Eksekusi handler yang sesuai
            return handler(request, *args, **kwargs)
        except Exception as e:
            print(traceback.format_exc())
            return _catch_error_details(e)
        
    def get_cust_loc(self, request, *args, **kwargs):
        # print(request.data)
        data_payload = request.data.get('data', {})
        cusid = data_payload.get('toko')
        mgr = data_payload.get('manager')
        spv = data_payload.get('supervisor')
        provinsi = data_payload.get('provinsi')
        kota = data_payload.get('kota')
        spg = data_payload.get('spg')
        nest = request.data.get('nest',0)
         # 1. Gunakan dictionary mapping pengganti if-elif
        MASTER_MAP = {
            "m_toko": {"col": "fn_cusid, fc_code, fv_toko", 
                       "distinct": False,
                       "filter":{"fc_hold":"F"}},
            "m_provinsi": {"col": "fv_provinsi", 
                           "distinct": True},
            "m_kota": {"col": "fv_kota", 
                       "distinct": True},
            "m_spv_cus": {"col": "fv_spv", 
                       "distinct": True,
                       "filter":{"fv_spv__not_in":["","-"]}},
            "m_mgr_cus": {"col": "fv_manager", 
                       "distinct": True,
                       "filter":{"fv_manager__not_in":["","-"]}},
            "m_filter_cus": {"col": "fn_cusid,fv_toko namecus, fc_code codecus, fv_spv, fv_manager, fv_provinsi, fv_kota", 
                       "distinct": True,
                       "filter":{"fv_spv__not_in":["","-"],
                                 "fv_manager__not_in":["","-"],
                                 "fn_cusid":cusid, 'fv_spv':spv, 'fv_manager':mgr,'fv_provinsi':provinsi,'fv_kota':kota
                                 }},
        }
        # Safe get dari request payload
        req_master = request.data.get('get') if isinstance(request.data, dict) else None
        # 2. Validasi input: Jika parameter 'get' tidak dikenali / kosong
        if req_master not in MASTER_MAP:
            return JsonResponse({
                "status": "error",
                "message": f"Parameter 'get' tidak valid atau tidak diizinkan. Pilih dari: {list(MASTER_MAP.keys())}"
            }, status=400)

        # 3. Ambil konfigurasi kolom berdasarkan mapping
        config = MASTER_MAP[req_master]
        custom_filters = config.get("filter", {})
        # 4. Eksekusi query service
        df_cust = self.df_cust_(
            colom=config["col"], 
            distinct=config["distinct"], 
            queri=True,
            ** custom_filters
        )
        if req_master == "m_toko":
            nested_data = {
                    "m_toko": df_cust.replace({np.nan: None}).to_dict(orient='records'),
                }

            return JsonResponse({
                "status": "success",
                "data": nested_data
            }, safe=False)
        list_tokospg=list_to_sql_in(df_cust,'fn_cusid',as_list=True)
        filters_toko = {"t__fn_cusid__in": list_tokospg}
        if spg:
            filters_toko["t__fn_userid__in"] = spg if isinstance(spg, (list, tuple)) else [spg]
        df_toko_hendle = self.df_toko_handle_(
            colom="UPPER(e.fv_nameemployees) AS spg, t.fn_cusid, e.fn_userid", 
            distinct=True, 
            queri=False,
            order_by='e.fv_nameemployees',
            **filters_toko
        )
        # 5. Konversi NaN ke None (agar di JsonResponse ter-serialize menjadi null)
        if int(nest)!=1:
            df_cust=df_cust.merge(df_toko_hendle,how='left',on='fn_cusid')
            records = df_cust.replace({np.nan: None}).to_dict(orient='records')
            return JsonResponse({
                "detailAbsen": records
            }, safe=False) 
        if df_cust.empty:
            nested_data = {
                "m_toko": [],
                "m_provinsi": [],
                "m_kota": [],
                "m_spv": [],
                "m_mgr": [],
                "m_spg":[]
            }
        else:
            # 1. m_toko: Ambil kolom toko dan ubah ke list of dict
            m_toko = df_cust[['fn_cusid', 'namecus', 'codecus']].drop_duplicates().to_dict(orient='records')

            # 2. m_provinsi, m_kota, m_spv, m_mgr: Ambil nilai unik dan buang nilai None/kosong
            m_provinsi = [p for p in df_cust['fv_provinsi'].dropna().unique().tolist() if p]
            m_kota = [k for k in df_cust['fv_kota'].dropna().unique().tolist() if k]
            m_spv = [s for s in df_cust['fv_spv'].dropna().unique().tolist() if s]
            m_mgr = [m for m in df_cust['fv_manager'].dropna().unique().tolist() if m]
            m_spg = (df_toko_hendle.dropna().drop_duplicates().to_dict(orient='records'))

            # 3. Gabungkan ke dalam satu dictionary nested
            nested_data = {
                "m_toko": m_toko,
                "m_provinsi": m_provinsi,
                "m_kota": m_kota,
                "m_spv": m_spv,
                "m_mgr": m_mgr,
                "m_spg":m_spg
            }

        return JsonResponse({
            "status": "success",
            "data": nested_data
        }, safe=False)
    
    def get_employee_live(self, request, *args, **kwargs):
         # 1. Gunakan dictionary mapping pengganti if-elif
        MASTER_MAP = {
            "m_spg_aktif": {
                "col": "emp.fn_idemployees, emp.fn_userid, emp.fv_nameemployees", 
                "distinct": False,
                "filters": {
                    "emp.fc_hold": "F",
                    "u.fc_hold": "F",
                    "u.fn_userlevel": [1, 8, 13]  # Sesuaikan alias tabel jika fn_userlevel ada di user_tb
                }
            },
            "m_spv_aktif": {
                "col": "emp.fn_idemployees, emp.fn_userid, emp.fv_nameemployees", 
                "distinct": False,
                "filters": {
                    "emp.fc_hold": "F",
                    "u.fc_hold": "F",
                    "u.fn_userlevel": [2, 12]
                }
            },
        }
        # Safe get dari request payload
        req_master = request.data.get('get') if isinstance(request.data, dict) else None
        # 2. Validasi input: Jika parameter 'get' tidak dikenali / kosong
        if req_master not in MASTER_MAP:
            return JsonResponse({
                "status": "error",
                "message": f"Parameter 'get' tidak valid atau tidak diizinkan. Pilih dari: {list(MASTER_MAP.keys())}"
            }, status=400)
        # 3. Ambil konfigurasi kolom berdasarkan mapping
        config = MASTER_MAP[req_master]
        # 4. Eksekusi query service
        df_cust = self.df_employee_(
            colom=config["col"], 
            distinct=config["distinct"],
            queri=False,
            **config["filters"]
        )
        # 5. Konversi NaN ke None (agar di JsonResponse ter-serialize menjadi null)
        # where(pd.notnull(df_cust), None) atau replace NaN murni
        records = df_cust.replace({np.nan: None}).to_dict(orient='records')
        return JsonResponse({
            "detailAbsen": records
        }, safe=False)
    
    
    def get_akses_user_cus_employee_live(self, request, *args, **kwargs):
        data_payload = request.data.get('data', {})
        user_id = data_payload.get('userid')
        MASTER_MAP = {
            "m_akses_toko_by_user": {
                "col": "c.fn_cusid AS fn_cusid,c.fc_codecus AS kodetoko,c.fv_namecus AS namatoko", 
                "distinct": True,  # Sesuaikan alias tabel jika fn_userlevel ada di user_tb
                "filter": {
                    "ts.fn_userid__in": [user_id],}
                },
            # "m_spv_aktif": {
            #     "col": "emp.fn_idemployees, emp.fn_userid, emp.fv_nameemployees", 
            #     "distinct": False,
            #     "filters": {
            #         "emp.fc_hold": "F",
            #         "u.fc_hold": "F",
            #         "u.fn_userlevel": [2, 12]
            #     }
            # },
        }
        # Safe get dari request payload
        req_master = request.data.get('get') if isinstance(request.data, dict) else None
        # 2. Validasi input: Jika parameter 'get' tidak dikenali / kosong
        if req_master not in MASTER_MAP:
            return JsonResponse({
                "status": "error",
                "message": f"Parameter 'get' tidak valid atau tidak diizinkan. Pilih dari: {list(MASTER_MAP.keys())}"
            }, status=400)

        # 3. Ambil konfigurasi kolom berdasarkan mapping
        config = MASTER_MAP[req_master]
        custom_filters = config.get("filter", {})
        df_toko_hendle=self.df_toko_handle_(
            colom=config["col"], 
            distinct=config["distinct"], 
            queri=False,
            ** custom_filters
        )
        records = df_toko_hendle.replace({np.nan: None}).to_dict(orient='records')
        return JsonResponse({
            "toko_hendle": records
        }, safe=False)

    def get_bank(self, request, *args, **kwargs):
        # data_payload = request.data.get('data', {})
        # user_id = data_payload.get('userid')
        # MASTER_MAP = {
        #     "m_bank": {
        #         }}
        # # Safe get dari request payload
        # req_master = request.data.get('get') if isinstance(request.data, dict) else None
        # # 2. Validasi input: Jika parameter 'get' tidak dikenali / kosong
        # if req_master not in MASTER_MAP:
        #     return JsonResponse({
        #         "status": "error",
        #         "message": f"Parameter 'get' tidak valid atau tidak diizinkan. Pilih dari: {list(MASTER_MAP.keys())}"
        #     }, status=400)

        # # 3. Ambil konfigurasi kolom berdasarkan mapping
        # config = MASTER_MAP[req_master]
        # custom_filters = config.get("filter", {})
        df=self.df_bank_(
            # colom=config["col"], 
            # distinct=config["distinct"], 
            # queri=False,
            # ** custom_filters
        )
        records = df.replace({np.nan: None}).to_dict(orient='records')
        return JsonResponse({
            "list_API": records
        }, safe=False)

    def get_ptkp(self, request, *args, **kwargs):
        df=self.df_ptkp_()
        records = df.replace({np.nan: None}).to_dict(orient='records')
        return JsonResponse({"list_API": records}, safe=False)

    def get_provinsi(self, request, *args, **kwargs):
        df=self.df_province_()
        records = df.replace({np.nan: None}).to_dict(orient='records')
        return JsonResponse({"list_API": records}, safe=False)
    
    def get_kota(self, request, *args, **kwargs):
        df=self.df_city_()
        records = df.replace({np.nan: None}).to_dict(orient='records')
        return JsonResponse({"list_API": records}, safe=False)
    
    def get_prov_kota(self, request, *args, **kwargs):
        df_kota=self.df_city_()
        df_prov=self.df_province_(colom="fv_provincename, fn_provinceid")
        df=df_kota.merge(df_prov,how='left',on='fn_provinceid')
        records = df.replace({np.nan: None}).to_dict(orient='records')
        return JsonResponse({
            "list_API": records
        }, safe=False)

    def get_userlevel(self, request, *args, **kwargs):
        # data_payload = request.data.get('data', {})
        # user_id = data_payload.get('userid')
        # MASTER_MAP = {
        #     "m_bank": {
        #         }}
        # # Safe get dari request payload
        # req_master = request.data.get('get') if isinstance(request.data, dict) else None
        # # 2. Validasi input: Jika parameter 'get' tidak dikenali / kosong
        # if req_master not in MASTER_MAP:
        #     return JsonResponse({
        #         "status": "error",
        #         "message": f"Parameter 'get' tidak valid atau tidak diizinkan. Pilih dari: {list(MASTER_MAP.keys())}"
        #     }, status=400)

        # # 3. Ambil konfigurasi kolom berdasarkan mapping
        # config = MASTER_MAP[req_master]
        # custom_filters = config.get("filter", {})
        df=self.df_userlevel_(
            # colom=config["col"], 
            # distinct=config["distinct"], 
            # queri=False,
            # ** custom_filters
        )
        records = df.replace({np.nan: None}).to_dict(orient='records')
        return JsonResponse({
            "list_API": records
        }, safe=False)
    
    def get_alamat(self, request, *args, **kwargs):
        df=self.df_alamat()
        records = df.replace({np.nan: None}).to_dict(orient='records')
        return JsonResponse({"list_API": records}, safe=False)
    
    def get_a_prov(self, request, *args, **kwargs):
        df=self.df_prov_()
        records = df.replace({np.nan: None}).to_dict(orient='records')
        return JsonResponse({"list_API": records}, safe=False)

    def get_a_kabkot(self, request, *args, **kwargs):
        data_payload = request.data.get('data', {})
        prov_id = data_payload.get('prov_id')
        df=self.df_kabkot_(fn_provid=prov_id)
        records = df.replace({np.nan: None}).to_dict(orient='records')
        return JsonResponse({"list_API": records}, safe=False)
    
    def get_a_kec(self, request, *args, **kwargs):
        data_payload = request.data.get('data', {})
        kabkot_id = data_payload.get('kabkot_id')
        df=self.df_kec_(fv_kabkota_id=kabkot_id)
        records = df.replace({np.nan: None}).to_dict(orient='records')
        return JsonResponse({"list_API": records}, safe=False)
    
    def get_a_keldes(self, request, *args, **kwargs):
        data_payload = request.data.get('data', {})
        kec_id = data_payload.get('kec_id')
        df=self.df_keldes_(fv_kec_id=kec_id)
        records = df.replace({np.nan: None}).to_dict(orient='records')
        return JsonResponse({"list_API": records}, safe=False)