
# from xhtml2pdf import pisa
import xlsxwriter
from xhtml2pdf import pisa
# from weasyprint import HTML
import pandas as pd
import numpy as np
from datetime import datetime
from zoneinfo import ZoneInfo
from timezonefinder import TimezoneFinder
import sys
import traceback
import json
import hashlib
import re
from datetime import datetime, timedelta, date
import os
import io
import jwt
from jwt.exceptions import InvalidSignatureError, ExpiredSignatureError
from io import BytesIO

from django.conf import settings
from django.shortcuts import render, redirect
from django.http import JsonResponse, HttpResponseBadRequest, HttpResponse
from django.views import View
from django.utils.decorators import method_decorator
from django.utils import timezone
from django.views.decorators.csrf import csrf_exempt
from django.template.loader import render_to_string
from django.db import connections, transaction
from django.contrib import messages
from dateutil import parser
from services.models_hris import (UserTb,TAksesusercust,MCusStay,EmployeesTb,EmployeesspgTb,
                                  HsEmployeesTb,HsUserTb,ApprovalAbs)
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status

from core.db import DatabaseConnector
from services.get_utils_API import (print_debug_tags,list_to_sql_in)
from services.Queries_API import (df__customers_pb_)
from services.Query_bronze import DataLayerMixin

dbLoad = 'default'
dbPRoll = 'payroll'
dbSave = 'local'
dbPRS = 'payroll_live'

tf = TimezoneFinder()
now = datetime.now()
wib_offset = (
    now.astimezone(ZoneInfo('Asia/Jakarta')).utcoffset().total_seconds() / 3600
)
def _catch_error_details(exception_obj):
        """Helper internal untuk melacak file, fungsi, dan baris tempat error terjadi"""
        exc_type, _, exc_tb = sys.exc_info()
        tb_details = traceback.extract_tb(exc_tb)[-1]
        # 1. Ambil traceback bawaan python
        full_traceback = traceback.format_exc()
        
        # 2. Pola untuk mencari 'views.py", line <angka>' tanpa peduli besar/kecil huruf
        #    \d+ artinya mencari kecocokan angka berapa pun (nomor baris)
        pola = r'(views\.py", line \d+)'
        
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

def _get_client_ip(request) -> str:
    """Mengambil IP address client dari request header."""
    x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')
    if x_forwarded_for:
        # Mengambil IP pertama jika ada multiple IP di X-Forwarded-For
        ip = x_forwarded_for.split(',')[0].strip()
    else:
        ip = request.META.get('REMOTE_ADDR', '')
    return ip

def parse_datein(date_str):
    """
    Mengubah berbagai format string tanggal ("2026-08-20", "20-08-2026", dll)
    menjadi datetime object yang timezone-aware.
    """
    if not date_str or not str(date_str).strip():
        return timezone.now()
    try:
        # parser.parse otomatis mengenali format 'YYYY-MM-DD', 'DD-MM-YYYY', 'YYYY/MM/DD', dll.
        parsed_dt = parser.parse(str(date_str).strip())
        return timezone.make_aware(parsed_dt) if timezone.is_naive(parsed_dt) else parsed_dt
    except (ValueError, TypeError):
        # Fallback ke waktu sekarang jika string format tidak valid
        return datetime.now()

def parse_date_nullable(date_str):
    """
    Konversi string tanggal dari JSON menjadi datetime/date.
    Mengembalikan None jika input kosong.
    """
    if not date_str or not str(date_str).strip():
        return None
    try:
        parsed_dt = parser.parse(str(date_str).strip())
        return timezone.make_aware(parsed_dt) if timezone.is_naive(parsed_dt) else parsed_dt
    except (ValueError, TypeError):
        return None

def parse_int_nullable(val):
    """Konversi nilai dari JSON ke Integer, mengembalikan None jika kosong."""
    if val is None or val == '':
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None

def fd_date_to_date_only(dt):
    """Helper konversi DateTime ke Date saja untuk kolom bertipe Date di MySQL (seperti fd_tgllahir)"""
    return dt.date() if dt else None
  
# 2. Fungsi Helper untuk ekstrak dan hitung selisih jam
def hitung_selisih_wib(posisi_str):
    if pd.isna(posisi_str) or not isinstance(posisi_str, str):
        return None
    try:
        lat_str, lng_str = posisi_str.strip().split()
        lat, lng = float(lat_str), float(lng_str)
    except (ValueError, AttributeError):
        return None
    tz_str = tf.timezone_at(lat=lat, lng=lng)
    if not tz_str:
        return None
    target_offset = (
        now.astimezone(ZoneInfo(tz_str)).utcoffset().total_seconds() / 3600
    )
    return target_offset - wib_offset

@method_decorator(csrf_exempt, name='dispatch')
class LoginAPIPayrollView(View):    
    def _format_html_list(self, item, months, is_approval_view=False):
        """Helper untuk standarisasi output HTML pengajuan & approval"""
        st = 'fa fa-thumbs-up text-success' if item['status'] == 'Y' else ('fa fa-thumbs-down text-danger' if item['status'] == 'N' else '')
        mulai = item['mulai']
        sampai = item['sampai']
        # --- PARSING MULAI ---
        if isinstance(mulai, str):
            if mulai == '0000-00-00 00:00:00' or mulai.startswith('0000-00'):
                mulai = None
            else:
                try:
                    mulai = datetime.strptime(mulai, '%Y-%m-%d %H:%M:%S')
                except ValueError:
                    try:
                        mulai = datetime.strptime(mulai, '%Y-%m-%d')
                    except ValueError:
                        mulai = None
        # --- PARSING SAMPAI ---
        if isinstance(sampai, str):
            if sampai == '0000-00-00 00:00:00' or sampai.startswith('0000-00'):
                sampai = None
            else:
                try:
                    sampai = datetime.strptime(sampai, '%Y-%m-%d %H:%M:%S')
                except ValueError:
                    try:
                        sampai = datetime.strptime(sampai, '%Y-%m-%d')
                    except ValueError:
                        sampai = None

        # --- ANTISIPASI JIKA DATETIME ADALAH NONE (Agar tidak strftime() error) ---
        # Buat format teks string atau default text jika data corrupt/kosong
        str_mulai = f"{mulai.strftime('%d')} {months.get(mulai.strftime('%F'), mulai.strftime('%B'))} {mulai.strftime('%Y')}" if mulai else "-"
        str_sampai = f"{sampai.strftime('%d')} {months.get(sampai.strftime('%F'), sampai.strftime('%B'))} {sampai.strftime('%Y')}" if sampai else "-"
        jam_mulai = mulai.strftime('%H:%M') if mulai else "--:--"
        jam_sampai = sampai.strftime('%H:%M') if sampai else "--:--"

        # --- GENERATE HTML BERDASARKAN JENIS ---
        if item['jenis'] == 'lembur':
            durasi_html = f"""
                <li class="list-group-item d-flex justify-content-between align-items-center p-1"><span>Tgl lembur:</span><span>{str_mulai}</span></li>
                <li class="list-group-item d-flex justify-content-between align-items-center p-1"><span>Waktu:</span><span>{jam_mulai} - {jam_sampai}</span></li>
            """
        else:
            label_mulai = "Tgl off awal:" if item['jenis'] == 'tukar off' else "Mulai tgl:"
            label_sampai = "Tukar off tgl:" if item['jenis'] == 'tukar off' else "Sampai tgl:"
            durasi_html = f"""
                <li class="list-group-item d-flex justify-content-between align-items-center p-1"><span>{label_mulai}</span><span>{str_mulai}</span></li>
                <li class="list-group-item d-flex justify-content-between align-items-center p-1"><span>{label_sampai}</span><span>{str_sampai}</span></li>
            """

        header_html = ""
        if is_approval_view:
            header_html = f"""
                <li class="list-group-item d-flex justify-content-between align-items-start p-1"><span>{item.get('kodetoko','')}</span><span>{item.get('namatoko','')}</span></li>
                <li class="list-group-item d-flex justify-content-between align-items-start p-1"><span>Nama:</span><span>{str(item.get('spg','')).lower().title()}</span></li>
            """

        status_approval = 'menunggu' if item['status'] == 'F' else ('disetujui' if item['status'] == 'Y' else 'ditolak')
        warna_status = 'bg-secondary' if item['status'] == 'F' else ('bg-success' if item['status'] == 'Y' else 'bg-danger')
        catatan = (item.get('note') or '').lower().capitalize()

        ul = f"""<ul class="list-group">
            {header_html}
            <li class="list-group-item d-flex justify-content-between align-items-start p-1">
                <div><i class="{st}"></i> Pengajuan {str(item['jenis']).upper()}</div>
            </li>
            <li class="list-group-item d-flex justify-content-start align-items-center p-1">{item['keterangan']}</li>
            {durasi_html}
            <li class="list-group-item d-flex justify-content-between align-items-center p-1 {warna_status} font-weight-bolder"><span>Status:</span><span>{status_approval}</span></li>
            <li class="list-group-item d-flex justify-content-between align-items-center p-1 {warna_status} font-weight-bolder"><span>Catatan atasan:</span><span>{catatan}</span></li>
        """

        if is_approval_view and item['status'] == 'F':
            ul += f"""
                <li class="list-group-item d-flex justify-content-between align-items-center {warna_status}">
                    <button class="btn btn-sm btn-danger rounded clickReject" data-action="{item['no']}_N" onclick="doReject(this)">Reject</button>
                    <button class="btn btn-sm btn-success rounded clickApprove" data-action="{item['no']}_Y" onclick="doApprove(this)">Approve</button>
                </li>
            """
        ul += "</ul>"
        return ul
    

    def post(self, request):
        try:
            body = json.loads(request.body)
            username = body.get('username', '')
            password = body.get('password', '')
            tabs = body.get('tabs', 'tabs_home') # Default 'tabs_home' jika tidak dikirim
            username_lower = username.lower()

            password_md5 = hashlib.md5(password.encode('utf-8')).hexdigest()
            
            check_level_query = "SELECT fn_userlevel, fn_userid FROM user_tb WHERE fv_username = %s AND fv_password_web = %s LIMIT 1"
            user_level_data = DatabaseConnector.execute_raw_sql(query=check_level_query, params=[username, password_md5], using='local', fetch_one=True)
            
            if not user_level_data:
                return JsonResponse({'success': False, 'message': 'Username atau Password salah'}, status=401)
            
            is_staff = user_level_data.get('fn_userlevel')
            staff_levels = [1, 2, 8, 12, 13]
            super_user=user_level_data.get('fn_userid')
            # Pilih query berdasarkan level staff/non-staff
            if is_staff in staff_levels:
                query = """
                    SELECT user.fv_username AS userlogin, user.fn_userid AS userid, user.fn_userlevel AS userlevel,
                           emp.fv_nameemployees AS fullname, emp.fn_idemployees AS empid, dp.fv_namedepart AS depart,
                           t.fn_cusid AS stay, ct.fc_codecus AS storecode, ct.fv_namecus AS storename
                    FROM user_tb user
                    LEFT JOIN employees_tb emp ON user.fn_userid = emp.fn_userid
                    INNER JOIN department_tb dp ON emp.fn_iddepart = dp.fn_iddepart
                    LEFT JOIN t_aksesusercust t ON emp.fn_userid = t.fn_userid
                    LEFT JOIN customer_tb ct ON t.fn_cusid = ct.fn_cusid
                    WHERE user.fv_username = %s AND user.fv_password_web = %s AND user.fc_hold = 'F' LIMIT 1
                """
            else:
                query = """
                    SELECT user.fv_username AS userlogin, user.fn_userid AS userid, user.fn_userlevel AS userlevel,
                           emp.fv_nameemployees AS fullname, emp.fn_idemployees AS empid, dp.fv_namedepart AS depart
                    FROM user_tb user
                    LEFT JOIN employees_tb emp ON user.fn_userid = emp.fn_userid
                    INNER JOIN department_tb dp ON emp.fn_iddepart = dp.fn_iddepart
                    WHERE user.fv_username = %s AND user.fv_password_web = %s AND user.fc_hold = 'F' LIMIT 1
                """
            user_records = DatabaseConnector.execute_raw_sql(query=query, params=[username, password_md5], using='local', fetch_one=True)
            if user_records['depart'] not in ['INFORMATIC TECHNOLOGY','HRD']:
                return JsonResponse({'success': False, 'message': 'Akses ditolak'}, status=401)
            if user_records:
                user_level_raw = user_records.get('userlevel')
                user_records['userlevel'] = user_level_raw if user_level_raw in staff_levels else 0
                user_records['stay'] = user_records.get('stay') if user_records.get('stay') is not None else 0
                
                if user_records.get('storecode'):
                    user_records['store'] = f"{user_records.get('storecode')} {user_records.get('storename')}"
                expire_time = datetime.today() + timedelta(days=1)
                user_records['exp'] = int(expire_time.timestamp())

                clean_payload = {
                    'userid': int(user_records.get('userid')),
                    'userlevel': int(user_records.get('userlevel', 0)),
                    'userlogin': str(user_records.get('userlogin', '')),
                    'fullname': str(user_records.get('fullname', '')),
                    'exp': int((datetime.today() + timedelta(days=1)).timestamp()),
                    'stay':int(user_records['stay'])
                }
                token = jwt.encode(clean_payload, settings.SECRET_KEY, algorithm="HS256")
                
                return JsonResponse({
                    'success': True, 
                    'token': token,
                    'cek_data':clean_payload,
                })
            else:
                return JsonResponse({'success': False, 'message': 'User diblokir atau tidak aktif'}, status=401)

        except Exception as e:
            return _catch_error_details(e)
        
class AttendanceReportAPIView(DataLayerMixin,APIView):
    def get(self, request):
        """
        Mengambil detail absensi (Pengganti getAttendanceDetail)
        """
        try:
            type_param = request.GET.get('type', 'data')
            print(type_param)
            if type_param == 'detail':
                return self.get_attendance_detail(request, no=None)
            elif type_param == 'data':
                return self.get_attendance_data(request)
            else:
                return JsonResponse({'error': f"Type '{type_param}' tidak ditemukan"}, status=400)

        except Exception as e:
            return _catch_error_details(e)
         
    
    
    def post(self, request, *args, **kwargs):
        """
        Main routing post method berdasarkan payload 'type'.
        """
        # --- (Opsional) Validasi JWT Anda di sini sesuai contoh ---
        auth_header = request.headers.get('Authorization')
        if not auth_header:
            return JsonResponse({"message": "Akses ditolak, token tidak ditemukan!"}), 401
        token = auth_header.split(" ")[1].strip()
        try:
            decoded_payload = jwt.decode(token, settings.SECRET_KEY, algorithms=["HS256"])
        except InvalidSignatureError:
            return JsonResponse({'error': 'Token tidak valid / Signature failed'}, status=401)
        except ExpiredSignatureError:
            return JsonResponse({'error': 'Token sudah kedaluwarsa'}, status=401)
        # ---------------------------------------------------------

        # Mengambil payload data (mendukung DRF request.data maupun raw body)
        data = request.data if hasattr(request, 'data') else {}
        if not data:
            try:
                data = json.loads(request.body.decode('utf-8'))
            except Exception:
                data = request.POST
        # Membaca type parameter
        type_param = data.get('type')
        username = request.data.get('superuser',decoded_payload.get('userlogin'))
        try:
            # Routing berdasarkan type
            if type_param == 'insert':
                return self.post_insert(request, data, username)
            
            elif type_param == 'report':
                return self.post_report(request, data)
                
            elif type_param == 'update':
                return self.post_update(request, data, username)
                
            elif type_param == 'delete':
                return self.post_delete(request, data, username)
            elif type_param == 'approval_abs':
                return self.approval_abs(request, decoded_payload)
                
            else:
                return JsonResponse({
                    'status': 'error', 
                    'message': f"Method type '{type_param}' tidak valid atau tidak ditemukan."
                }, status=400)
                
        except Exception as e:
            return _catch_error_details(e)

    def get_attendance_data(self, request):
        def get_employees_local(level='spg', store='all', spv='all', spg=None, list_toko_ids=None, start_date=None, end_date=None, using='local'):
            """
            Mengambil data master employee & customer dari DB Local.
            """
            params = []
            where_conditions = []
            
            # Filter status aktif / tanggal keluar
            if start_date and end_date:
                where_conditions.append("((ut.fc_hold = 'F' AND et.fc_hold = 'F') OR (et.fc_hold = 'T' AND et.fd_dateout BETWEEN %s AND %s))")
                params.extend([start_date, end_date])
            else:
                where_conditions.append("(ut.fc_hold = 'F' AND et.fc_hold = 'F')")

            if level == 'supervisor':
                dept_user_filter = "(et.fn_iddepart IN (8,20) AND ut.fn_userlevel IN (2,12) OR ut.fn_userid = 3085)"
                if spv != 'all':
                    where_conditions.append("et.fn_userid = %s")
                    params.append(int(spv))
            else:
                dept_user_filter = "et.fn_iddepart IN (8,20) AND ut.fn_userlevel IN (1,8,13)"
                if store != 'all':
                    where_conditions.append("ta.fn_cusid = %s")
                    params.append(int(store))
                if spg:
                    where_conditions.append("et.fn_userid = %s")
                    params.append(int(spg))

            where_sql = " AND ".join(where_conditions)
            
            # Select fc_codecus untuk join ke list_pic_marketing (payroll) nantinya
            query = f"""
                SELECT 
                    et.fn_userid,
                    et.fc_codeemployees AS codeemployees,
                    et.fv_nameemployees AS nameemployees,
                    DATE_FORMAT(et.fd_datein, '%%Y-%%m-%%d') AS datein,
                    DATE_FORMAT(et.fd_dateout, '%%Y-%%m-%%d') AS dateout,
                    ct.fc_codecus
                FROM employees_tb et
                INNER JOIN user_tb ut ON et.fn_userid = ut.fn_userid AND {dept_user_filter}
                LEFT JOIN t_aksesusercust ta ON ut.fn_userid = ta.fn_userid
                LEFT JOIN customer_tb ct ON ta.fn_cusid = ct.fn_cusid
                WHERE {where_sql}
                GROUP BY et.fn_userid
                ORDER BY et.fv_nameemployees ASC
            """
            
            results = DatabaseConnector.execute_raw_sql(query, params=params, using=using, p_queri=False)
            return pd.DataFrame(results) if results else pd.DataFrame()
        level = request.GET.get('level')
        action = request.GET.get('action', '')
        start_date = request.GET.get('start_date')
        end_date = request.GET.get('end_date')
        cusid = request.GET.get('toko')
        mgr = request.GET.get('manager')
        spv = request.GET.get('supervisor')
        provinsi = request.GET.get('provinsi')
        kota = request.GET.get('kota')
        spg = request.GET.get('spg')
        
        if not start_date or not end_date:
            return JsonResponse({"detailAbsen": [], "rekapAbsen": []}, safe=False)

        # 1. AMBIL FILTER TOKO (DB Local)
        df_cus = self.df_cust_(
            colom='fn_cusid, fv_toko namecus, fc_code codecus, fv_spv, fv_manager, fv_provinsi, fv_kota',
            queri=False,
            **{'fn_cusid': cusid, 'fv_spv': spv, 'fv_manager': mgr, 'fv_provinsi': provinsi, 'fv_kota': kota}
        )
        
        list_toko_ids = df_cus['fn_cusid'].unique().tolist() if not df_cus.empty else None

        # 2. AMBIL MASTER DATA KARYAWAN (Full DB Local)
        df_emp = get_employees_local(
            level=level,
            store=request.GET.get('toko', 'all'),
            spv=request.GET.get('spv', 'all'),
            spg=spg,
            list_toko_ids=list_toko_ids,
            start_date=start_date,
            end_date=end_date,
            using='local'
        )

        if df_emp.empty:
            return JsonResponse({"detailAbsen": [], "rekapAbsen": []}, safe=False)

        # 3. HANYA TABEL `list_pic_marketing` YANG DIAMBIL DARI DB Payroll (Khusus Level Supervisor)
        if level == 'supervisor':
            query_lpm = """
                SELECT 
                    kode_toko_gis AS fc_codecus,
                    CASE 
                        WHEN manager = '-' AND chief_supervisor = '-' THEN '-'
                        WHEN manager = '-' AND chief_supervisor <> '-' THEN chief_supervisor
                        ELSE manager 
                    END AS storeManager
                FROM list_pic_marketing
            """
            lpm_list = DatabaseConnector.execute_raw_sql(query_lpm, using='payroll', p_queri=False)
            
            if lpm_list:
                df_lpm = pd.DataFrame(lpm_list).drop_duplicates(subset=['fc_codecus'])
                # Join data DB Local dan DB Payroll menggunakan Pandas (by fc_codecus / kode_toko_gis)
                df_emp = df_emp.merge(df_lpm, on='fc_codecus', how='left')
                df_emp['storeManager'] = df_emp['storeManager'].fillna('-')
            else:
                df_emp['storeManager'] = '-'
                
            # Drop kolom fc_codecus jika tidak diperlukan lagi di output akhir
            df_emp.drop(columns=['fc_codecus'], inplace=True, errors='ignore')
        ## Harus diganti ke 237 x
        df_custay=self.df_cus_stay_(colom='fn_userid,fn_cusid')
        df_emp=df_emp.merge(df_custay,how='left',on='fn_userid')
        df_emp=df_cus.merge(df_emp,how='right',on='fn_cusid')

        user_ids = df_emp['fn_userid'].tolist()
        # 2. AMBIL DATA ABSENSI MASAL
        format_strings = ', '.join(['%s'] * len(user_ids))
        params_att = [start_date, end_date] + user_ids
        ##
        query_att = f"""
            SELECT 
                fn_no,adc.fn_userid, DATE_FORMAT(adc.fd_tanggal, '%%Y-%%m-%%d') AS tanggal, adc.fc_status AS status, adc.fc_jenis AS jenis,
                adc.fc_foto->>'$.in' AS fotomasuk, adc.fc_foto->>'$.out' AS fotopulang,
                adc.fc_posisi->>'$.in' AS posisimasuk, adc.fc_posisi->>'$.out' AS posisipulang,
                adc.fd_create AS masuk, adc.fd_update AS pulang,
                CASE WHEN adc.fc_shift->>'$.in' = 'null' THEN '-' ELSE adc.fc_shift->>'$.in' END AS shift,
                CASE WHEN adc.fc_shift->>'$.out' = 'null' THEN '-' ELSE adc.fc_shift->>'$.out' END AS shiftpulang,
                CASE WHEN adc.fc_status = 'out' THEN TIME_FORMAT(TIMEDIFF(adc.fd_update, adc.fd_create), '%%H jam %%i menit') END AS durasi,
                adc.fc_penjualanharian->>'$.qty' AS qtysale, adc.fc_penjualanharian->>'$.value' AS valuesale,
                adc.fc_tempat->>'$.in' AS tempat_in, adc.fc_tempat->>'$.out' AS tempat_out, adc.fn_toko_in, adc.fn_toko_out
            FROM absensi_dps_copy adc
            WHERE adc.fd_tanggal BETWEEN %s AND %s AND adc.fn_userid IN ({format_strings}) AND adc.stat <> 'd'
        """
        
        
        att_list = DatabaseConnector.execute_raw_sql(query_att, params=params_att, using='payroll')
        """
        SELECT 
            fn_no,adc.fn_userid, DATE_FORMAT(adc.fd_tanggal, '%%Y-%%m-%%d') AS tanggal, adc.fc_status AS status, adc.fc_jenis AS jenis,
            adc.fc_foto->>'$.in' AS fotomasuk, adc.fc_foto->>'$.out' AS fotopulang,
            adc.fc_posisi->>'$.in' AS posisimasuk, adc.fc_posisi->>'$.out' AS posisipulang,
            adc.fd_create AS masuk, adc.fd_update AS pulang,
            CASE WHEN adc.fc_shift->>'$.in' = 'null' THEN '-' ELSE adc.fc_shift->>'$.in' END AS shift,
            CASE WHEN adc.fc_shift->>'$.out' = 'null' THEN '-' ELSE adc.fc_shift->>'$.out' END AS shiftpulang,
            CASE WHEN adc.fc_status = 'out' THEN TIME_FORMAT(TIMEDIFF(adc.fd_update, adc.fd_create), '%%H jam %%i menit') END AS durasi,
            adc.fc_penjualanharian->>'$.qty' AS qtysale, adc.fc_penjualanharian->>'$.value' AS valuesale,
            ct.fc_codecus AS fc_codecus_in, ct.fv_namecus AS fv_namecus_in, ct.fn_cusid AS fn_cusid_in, 
            cts.fc_codecus AS fc_codecus_out, cts.fv_namecus AS fv_namecus_out, cts.fn_cusid AS fn_cusid_out,
            adc.fc_tempat->>'$.in' AS tempat_in, adc.fc_tempat->>'$.out' AS tempat_out
        FROM absensi_dps_copy adc
        LEFT JOIN customer_tb ct ON adc.fn_toko_in = ct.fn_cusid
        LEFT JOIN customer_tb cts ON adc.fn_toko_out = cts.fn_cusid
        WHERE adc.fd_tanggal BETWEEN %s AND %s AND adc.fn_userid IN ({format_strings}) AND adc.stat <> 'd'
    """
        ## Harus diganti ke 237 x
        df_att = pd.DataFrame(att_list) if att_list else pd.DataFrame(columns=['fn_userid', 'tanggal', 'status', 'jenis'])
        df_cus=self.df_cust_(colom=("fn_cusid,fc_code,fv_toko"))
        df_att=df_att.merge(df_cus,how='left',left_on='fn_toko_in',right_on='fn_cusid').rename(columns={'fc_code':"fc_codecus_in",'fv_toko':"fv_namecus_in",}).drop(columns='fn_cusid')
        df_att=df_att.merge(df_cus,how='left',left_on='fn_toko_out',right_on='fn_cusid').rename(columns={'fc_code':"fc_codecus_out",'fv_toko':"fv_namecus_out",}).drop(columns='fn_cusid')
        
        # 3. GENERATE DATE RANGE MATRIX (Cross Join ala Analyst)
        date_range = pd.date_range(start=start_date, end=end_date).strftime('%Y-%m-%d').tolist()
        df_matrix = pd.MultiIndex.from_product([df_emp['fn_userid'], date_range], names=['fn_userid', 'tanggal']).to_frame().reset_index(drop=True)
        
        # Merge Master dengan Matrix Tanggal
        df_matrix = df_matrix.merge(df_emp, on='fn_userid', how='left')
        
        # Gabungkan dengan data Absensi Aktual (Mendukung multi-baris jika dalam 1 hari ada 'in' dan 'out')
        df_final_detail = df_matrix.merge(df_att, on=['fn_userid', 'tanggal'], how='left')

        # 4. LOGIKA ANALYTICS & REKAYASA FITUR (Numpy Vectorization)
        today_str = datetime.now().strftime('%Y-%m-%d')
        is_past_or_today = df_final_detail['tanggal'] <= today_str

        # ISI DULU DENGAN PENANDA SEMENTARA (Agar tidak error)
        df_final_detail['status'] = df_final_detail['status'].fillna('KOSONG')
        df_final_detail['jenis'] = df_final_detail['jenis'].fillna('KOSONG')

        # BERSIHKAN MENGGUNAKAN NP.WHERE SECARA VEKTOR (Aman & Super Cepat)
        df_final_detail['status'] = np.where(df_final_detail['status'] == 'KOSONG', np.where(is_past_or_today, 'alpa', 'belum absen'), df_final_detail['status'])
        df_final_detail['jenis'] = np.where(df_final_detail['jenis'] == 'KOSONG', np.where(is_past_or_today, 'alpa', 'belum absen'), df_final_detail['jenis'])
        
        # Imputasi kolom detail yang kosong lainnya
        fill_cols = ['fotomasuk', 'fotopulang', 'posisimasuk', 'posisipulang', 'masuk', 'pulang', 'shift', 'shiftpulang', 'durasi']
        df_final_detail[fill_cols] = df_final_detail[fill_cols].fillna('belum absen')

        # Kalkulasi Kolom Penjualan
        df_final_detail['qtysale'] = pd.to_numeric(df_final_detail['qtysale'], errors='coerce').fillna(0).astype(int)
        df_final_detail['valuesale'] = pd.to_numeric(df_final_detail['valuesale'], errors='coerce').fillna(0).astype(int)
        df_final_detail['penjualan'] = np.where(
            df_final_detail['status'] != 'out', '-',
            df_final_detail['qtysale'].astype(str) + '/' + df_final_detail['valuesale'].astype(str)
        )

        # 5. PEMROSESAN DATA REKAP (PANDAS RESHAPING)
        rekap_list = []
        
        # --- PINDAHKAN LOGIKA PIVOT KE LUAR IF AGAR BISA DIPAKAI EXCEL & PDF ---
        df_rekap_src = df_final_detail.copy()
        df_rekap_src['date_formatted'] = pd.to_datetime(df_rekap_src['tanggal']).dt.strftime('%d/%m/%Y')
        
        # Rule khusus masa aktif SPG lapangan
        if 'datein' in df_rekap_src.columns and 'dateout' in df_rekap_src.columns:
            invalid_in = df_rekap_src['datein'].notna() & (df_rekap_src['tanggal'] < df_rekap_src['datein'].astype(str))
            invalid_out = df_rekap_src['dateout'].notna() & (df_rekap_src['tanggal'] > df_rekap_src['dateout'].astype(str))
            df_rekap_src['jenis'] = np.where(invalid_in | invalid_out, '-', df_rekap_src['jenis'])

        # Pivot tanggal menjadi kolom horizontal
        df_pivot_status = df_rekap_src.pivot_table(
            index='fn_userid', columns='date_formatted', values='jenis', aggfunc='first'
        ).fillna('-')

        # Hitung Summary Per Kategori Absensi
        categories = ['absen', 'off', 'cuti', 'sakit', 'ijin', 'alpa']
        df_counts = df_rekap_src[df_rekap_src['jenis'].isin(categories)].groupby(['fn_userid', 'jenis']).size().unstack(fill_value=0)
        
        # Menjamin semua kolom kategori tersedia
        for cat in categories:
            if cat not in df_counts.columns:
                df_counts[cat] = 0
        if action == 'excel':
            df_rekap_src = df_final_detail.copy()
            df_rekap_src['date_formatted'] = pd.to_datetime(df_rekap_src['tanggal']).dt.strftime('%d/%m/%Y')
            
            # Rule khusus masa aktif SPG lapangan
            if 'datein' in df_rekap_src.columns and 'dateout' in df_rekap_src.columns:
                invalid_in = df_rekap_src['datein'].notna() & (df_rekap_src['tanggal'] < df_rekap_src['datein'].astype(str))
                invalid_out = df_rekap_src['dateout'].notna() & (df_rekap_src['tanggal'] > df_rekap_src['dateout'].astype(str))
                df_rekap_src['jenis'] = np.where(invalid_in | invalid_out, '-', df_rekap_src['jenis'])

            # Pivot tanggal menjadi kolom horizontal (Mengambil record status pertama jika ada in/out ganda)
            df_pivot_status = df_rekap_src.pivot_table(
                index='fn_userid', columns='date_formatted', values='jenis', aggfunc='first'
            ).fillna('-')

            # Hitung Summary Per Kategori Absensi menggunakan Groupby nominal
            categories = ['absen', 'off', 'cuti', 'sakit', 'ijin', 'alpa']
            df_counts = df_rekap_src[df_rekap_src['jenis'].isin(categories)].groupby(['fn_userid', 'jenis']).size().unstack(fill_value=0)
            # Menjamin semua kolom kategori tersedia di DataFrame hasil kalkulasi counts
            for cat in categories:
                if cat not in df_counts.columns:
                    df_counts[cat] = 0

            # Gabungkan master employee + status harian + total counts summary
            df_rekap_final = df_emp.merge(df_pivot_status, on='fn_userid', how='left').merge(df_counts, on='fn_userid', how='left').fillna(0)

            # Parsing DataFrame Rekap ke format Array-Dict sesuai spesifikasi frontend lama Anda
            tgl_cols = df_pivot_status.columns.tolist()
            for _, row in df_rekap_final.iterrows():
                row_dict = row.to_dict()
                base_rekap = {
                    'fn_userid': int(row_dict['fn_userid']),
                    'codeemployees': row_dict['codeemployees'],
                    'nameemployees': row_dict['nameemployees'],
                    'storeManager': row_dict['fv_manager'],
                    'status': {tgl: row_dict[tgl] for tgl in tgl_cols},
                    'total': {cat: int(row_dict[cat]) for cat in categories}
                }
                if level != 'supervisor':
                    base_rekap.update({
                        'codecus': row_dict.get('codecus', '-'),
                        'namecus': row_dict.get('namecus', '-'),
                        'storeSupervisor': row_dict.get('storeSupervisor', '-')
                    })
                rekap_list.append(base_rekap)

        # 6. SLICING & SINKRONISASI OUTPUT (Membatasi 100 baris data detail pertama)
        df_final_detail = df_final_detail.drop(columns=['datein', 'dateout'], errors='ignore')
        
        # Ambil 100 baris teratas DataFrame
        df_limited_detail = df_final_detail.fillna('')
        # Daftar kolom yang ingin diubah nilainya menjadi "-"
        kolom_target = [
            "fotomasuk",
            "fotopulang",
            "posisimasuk",
            "posisipulang",
            "masuk",
            "pulang",
        ]
        # ^ Sesuaikan daftar di atas dengan kolom-kolom yang ingin Anda ubah

        # Kondisi: jenis TIDAK SAMA DENGAN 'absen'
        kondisi = df_limited_detail["jenis"] != "absen"

        # Mengubah nilai pada kolom_target menjadi "-" untuk baris yang memenuhi kondisi
        df_limited_detail.loc[kondisi, kolom_target] = ""
        # Jika rekap excel aktif, pastikan sinkron hanya untuk user yang lolos di 100 baris detail pertama
        if action == 'excel':
            import io
            output = io.BytesIO()

            # 1. Persiapkan Data Rekap (Sheet 2)
            df_rekap_excel = df_emp.merge(df_pivot_status, on='fn_userid', how='left')\
                                .merge(df_counts, on='fn_userid', how='left')\
                                .fillna(0)

            # 2. Persiapkan Data Detail (Sheet 1)
            df_download_detail = df_final_detail.drop(columns=['datein', 'dateout', 'date_formatted'], errors='ignore')

            # Urutan kolom disesuaikan dengan header Excel
            columns_order = [
                'nameemployees', 'fv_spv', 'fv_manager', 'tanggal', 'jenis',
                'masuk', 'tempat_in', 'posisimasuk', 
                'pulang', 'tempat_out', 'posisipulang', 
                'durasi', 'penjualan'
            ]
            
            # Pastikan DataFrame hanya mengambil kolom yang dibutuhkan & sesuai urutan
            df_download_detail = df_download_detail.reindex(columns=columns_order)

            # 3. Gunakan ExcelWriter dengan engine xlsxwriter
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                workbook  = writer.book
                
                # --- PREPARE FORMATTING / STYLES ---
                header_format = workbook.add_format({
                    'bold': True,
                    'align': 'center',
                    'valign': 'vcenter',
                    'border': 1,
                    'text_wrap': True
                })
                
                cell_format = workbook.add_format({
                    'align': 'left',
                    'valign': 'vcenter',
                    'border': 1
                })
                
                center_cell_format = workbook.add_format({
                    'align': 'center',
                    'valign': 'vcenter',
                    'border': 1
                })

                # --- SHEET 1: DETAIL ABSENSI ---
                worksheet1 = workbook.add_worksheet('Detail Absensi')
                
                # A. Buat Header Bertingkat (Baris 0 & 1 -> Excel Row 1 & 2)
                # merge_range(first_row, first_col, last_row, last_col, data, format)
                worksheet1.merge_range(0, 0, 1, 0, 'No', header_format)
                worksheet1.merge_range(0, 1, 1, 1, 'Nama', header_format)
                worksheet1.merge_range(0, 2, 1, 2, 'SPV', header_format)
                worksheet1.merge_range(0, 3, 1, 3, 'Manager', header_format)
                worksheet1.merge_range(0, 4, 1, 4, 'Tanggal', header_format)
                worksheet1.merge_range(0, 5, 1, 5, 'Status', header_format)
                
                # Kelompok Absen Masuk (Merge Horizontal Kolom F - H)
                worksheet1.merge_range(0, 6, 0, 8, 'Absen Masuk', header_format)
                worksheet1.write(1, 6, 'Waktu', header_format)
                worksheet1.write(1, 7, 'Tempat', header_format)
                worksheet1.write(1, 8, 'Posisi', header_format)
                
                # Kelompok Absen Pulang (Merge Horizontal Kolom I - K)
                worksheet1.merge_range(0, 9, 0, 11, 'Absen Pulang', header_format)
                worksheet1.write(1, 9, 'Waktu', header_format)
                worksheet1.write(1, 10, 'Tempat', header_format)
                worksheet1.write(1, 11, 'Posisi', header_format)
                
                worksheet1.merge_range(0, 12, 1, 12, 'Durasi', header_format)
                worksheet1.merge_range(0, 13, 1, 13, 'penjualan', header_format)

                # B. Tulis Data (Mulai dari Baris Index 2 -> Excel Row 3)
                for row_idx, row_data in enumerate(df_download_detail.values):
                    excel_row = row_idx + 2
                    # Tulis Nomor Otomatis di Kolom 0 (A)
                    worksheet1.write(excel_row, 0, row_idx + 1, center_cell_format)
                    
                    # Tulis Nilai Baris Data
                    for col_idx, val in enumerate(row_data):
                        # +1 karena kolom 0 sudah diisi nomor
                        worksheet1.write(excel_row, col_idx + 1, val if pd.notnull(val) else "", cell_format)

                # --- SHEET 2: REKAP BULANAN ---
                df_rekap_excel.to_excel(writer, sheet_name='Rekap Bulanan', index=False)

                # --- AUTO FIT / COLUMN WIDTH ---
                for sheet in writer.sheets.values():
                    sheet.autofit()

            # 4. Kembalikan Response ke Frontend
            output.seek(0)
            filename = f"Laporan_Absensi_{start_date}_to_{end_date}.xlsx"
            
            response = HttpResponse(
                output.getvalue(), 
                content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            )
            response['Content-Disposition'] = f'attachment; filename="{filename}"'
            return response

        if action == 'pdf' and level == 'supervisor':
            import os
            import io
            import base64
            from PIL import Image
            from django.conf import settings

            def get_compressed_image_base64(relative_path, max_size=(100, 120), quality=60):
                """
                Cek MEDIA_ROOT_OLD -> MEDIA_ROOT. 
                Jika file ada, resize & kompres di memori, lalu kembalikan format Base64.
                """
                if not relative_path or relative_path in ['belum absen', '', None]:
                    return ""
                clean_path = str(relative_path).lstrip('/')
                # 1. Cari file fisik (OLD -> NEW)
                old_root = getattr(settings, 'MEDIA_ROOT_OLD', os.path.join(settings.BASE_DIR, 'storage_old'))
                full_path = os.path.join(old_root, clean_path)
                if not os.path.exists(full_path):
                    full_path = os.path.join(settings.MEDIA_ROOT, clean_path)
                    if not os.path.exists(full_path):
                        return ""  # File tidak ditemukan di kedua lokasi
                # 2. Kompresi & Resize menggunakan Pillow
                try:
                    with Image.open(full_path) as img:
                        # Konversi format gambar jika berupa RGBA/P ke RGB agar bisa disimpan sebagai JPEG
                        if img.mode in ("RGBA", "P"):
                            img = img.convert("RGB")
                        # Resize gambar dengan mempertahankan aspect ratio
                        img.thumbnail(max_size, Image.Resampling.LANCZOS)
                        # Simpan ke memory buffer (io.BytesIO)
                        buffer = io.BytesIO()
                        img.save(buffer, format="JPEG", quality=quality, optimize=True)
                        # Encode ke format Base64
                        img_str = base64.b64encode(buffer.getvalue()).decode('utf-8')
                        return f"data:image/jpeg;base64,{img_str}"
                except Exception as e:
                    print(f"Error compressing image {full_path}: {e}")
                    return ""
            
            df_spv_pdf = df_limited_detail.copy()
            raw_detail = df_spv_pdf.to_dict(orient='records')
            detail_data = []

            for item in raw_detail:
                # Panggil helper untuk foto masuk & foto pulang
                foto_in_path = get_compressed_image_base64(item.get('fotomasuk'))
                foto_out_path = get_compressed_image_base64(item.get('fotopulang'))

                detail_data.append({
                    'nameemployees': item.get('nameemployees', '-'),
                    'tanggal': item.get('tanggal', '-'),
                    
                    # --- ABSEN MASUK ---
                    'masuk': item.get('masuk', '-'),
                    'tempat_in': item.get('tempat_in', '-'),
                    'kegiatan_in': item.get('shift', '-'),
                    'radius_in': item.get('posisimasuk', '-'),
                    'foto_in': foto_in_path,  # Berisi path absolut file yang valid
                    
                    # --- ABSEN KELUAR ---
                    'pulang': item.get('pulang', '-'),
                    'tempat_out': item.get('tempat_out', '-'),
                    'kegiatan_out': item.get('shiftpulang', '-'),
                    'radius_out': item.get('posisipulang', '-'),
                    'foto_out': foto_out_path, # Berisi path absolut file yang valid
                    
                    'durasi': item.get('durasi', '-')
                })

            context = {
                'detail': detail_data,
                'start_date': start_date,
                'end_date': end_date
            }

            html_string = render_to_string('payroll/pdf_detail_spv.html', context)
            
            response = HttpResponse(content_type='application/pdf')
            response['Content-Disposition'] = f'attachment; filename="Detail_Absen_SPV_{start_date}.pdf"'
            
            pisa_status = pisa.CreatePDF(html_string, dest=response)
            if pisa_status.err:
                return JsonResponse({"detail": "Gagal membuat PDF"}, status=500)
                
            return response
        # A. Hitung selisih jam dari koordinat
        df_limited_detail['selisih_wib_masuk'] = df_limited_detail['posisimasuk'].apply(hitung_selisih_wib)
        df_limited_detail['selisih_wib_pulang'] = df_limited_detail['posisipulang'].apply(hitung_selisih_wib)

        # B. Konversi string masuk & pulang ke format datetime Pandas
        df_limited_detail['masuk_dt'] = pd.to_datetime(
            df_limited_detail['masuk'], errors='coerce'
        )
        df_limited_detail['pulang_dt'] = pd.to_datetime(
            df_limited_detail['pulang'], errors='coerce'
        )

        # C. Tambahkan selisih jam ke kolom datetime menggunakan pd.to_timedelta
        #    (Waktu lokal disesuaikan dengan selisih terhadap WIB)
        df_limited_detail['masuk_sesuai_wib'] = df_limited_detail[
            'masuk_dt'
        ] + pd.to_timedelta(df_limited_detail['selisih_wib_masuk'], unit='h')
        df_limited_detail['pulang_sesuai_wib'] = df_limited_detail[
            'pulang_dt'
        ] + pd.to_timedelta(df_limited_detail['selisih_wib_pulang'], unit='h')

        df_limited_detail['masuk'] = (
            df_limited_detail['masuk_sesuai_wib']
            .dt.strftime('%Y-%m-%dT%H:%M:%S')
            .fillna(df_limited_detail['masuk'])
        )

        df_limited_detail['pulang'] = (
            df_limited_detail['pulang_sesuai_wib']
            .dt.strftime('%Y-%m-%dT%H:%M:%S')
            .fillna(df_limited_detail['pulang'])
        )

        # 4. Bersihkan kolom temporary datetime agar DataFrame tetap rapi
        df_limited_detail.drop(
            columns=['masuk_dt', 'pulang_dt', 'masuk_sesuai_wib', 'pulang_sesuai_wib','selisih_wib_pulang','selisih_wib_masuk'],
            inplace=True,
            errors='ignore',
        )
        rekap_summary = {
            "total_rows": len(df_limited_detail),
            "total_columns": len(df_limited_detail.columns),
            "columns": df_limited_detail.dtypes.astype(str).to_dict()  # Nama kolom & tipe datanya
        }
        return JsonResponse({
            "rekapAbsen": rekap_summary,
            "detailAbsen": df_limited_detail.replace({np.nan: None}).to_dict(orient='records'),
            # 'userlist':user_list
        }, safe=False)
    
    def get_attendance_detail(self,data, level, action):
        if not no:
            return Response({'message': 'ID (no) diperlukan'}, status=status.HTTP_400_BAD_REQUEST)
            
        try:
            # Menggunakan raw SQL/QueryBuilder ala Django untuk performa optimal 
            # Mengambil data dari absensi_dps_copy (a) dan join ke employees_tb (e)
            with connection.cursor() as cursor:
                query = """
                    SELECT 
                        a.*, 
                        e.fv_nameemployees,
                        a.fc_tempat->>'$.in' as fc_tempat_in,
                        a.fc_tempat->>'$.out' as fc_tempat_out,
                        a.fc_shift->>'$.in' as fc_shift_in,
                        a.fc_shift->>'$.out' as fc_shift_out
                    FROM absensi_dps_copy a
                    INNER JOIN employees_tb e ON a.fn_userid = e.fn_userid
                    WHERE a.fn_no = %s
                    LIMIT 1
                """
                cursor.execute(query, [no])
                columns = [col[0] for col in cursor.description]
                row = cursor.fetchone()
                
                if not row:
                    return Response({'message': 'Data tidak ditemukan'}, status=status.HTTP_404_NOT_FOUND)
                
                # Mapping ke bentuk dictionary
                result = dict(zip(columns, row))
                return Response(result, status=status.HTTP_200_OK)
                
        except Exception as e:
            return Response({'message': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    # -------------------------------------------------------------------------
    # SUB-METHODS (HANDLERS)
    # -------------------------------------------------------------------------
    def post_insert(self, request, data, username):
        """Handler untuk Create/Insert Absensi (Versi Teroptimasi)"""
        
        # 1. Parsing parameter dryrun (mendukung integer 1/0, boolean, dan string)
        is_dryrun = str(data.get('dryrun', False)).lower() in ['true', '1']
        
        # PENTING: Gunakan variabel database connection untuk konsistensi
        db_name = dbPRoll  # Pastikan dbPRoll sudah terdefinisi di class/file ini

        try:
            # PENTING: Arahkan atomic ke database yang tepat agar rollback berfungsi!
            with transaction.atomic(using=db_name):
                
                # 2. Ekstraksi Dasar
                fc_jenis = data.get('fc_jenis', '')
                is_absen = (fc_jenis == 'absen')
                
                # 3. Transformasi Data yang Lebih Bersih (Ternary Operator)
                fc_status = 'out' if is_absen else fc_jenis
                
                # Ambil langsung dictionary fc_tempat dari payload
                fc_tempat_raw = data.get('fc_tempat', {})
                fc_tempat = fc_tempat_raw if is_absen else {'in': fc_jenis, 'out': fc_jenis}
                
                fn_toko_in = data.get('fn_in', 0) if is_absen else 0
                fn_toko_out = data.get('fn_out', 0) if is_absen else 0
                
                # Hindari dump JSON berulang untuk variabel yang isinya sama
                default_dict = {'in': '', 'out': ''} if is_absen else {'in': fc_jenis, 'out': fc_jenis}
                default_json_str = json.dumps(default_dict)
                
                # Timestamp creation
                now = datetime.now() 
                fd_create_raw = data.get('fd_create')
                fd_create = datetime.fromisoformat(fd_create_raw) if is_absen and fd_create_raw else now

                # 4. Eksekusi Insert
                insert_query = """
                    INSERT INTO absensi_dps (
                        fn_userid, fd_tanggal, fc_jenis, fc_status, fc_tempat, 
                        fn_toko_in, fn_toko_out, fc_shift, fc_posisi, fc_foto, 
                        fd_create, stat, fc_jam
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                params = [
                    data.get('fn_userid'), 
                    data.get('fd_tanggal'), 
                    fc_jenis, 
                    fc_status,
                    json.dumps(fc_tempat), 
                    fn_toko_in, 
                    fn_toko_out, 
                    json.dumps(data.get('fc_shift', {})),
                    default_json_str,  # fc_posisi
                    default_json_str,  # fc_foto
                    fd_create, 
                    'i',
                    json.dumps(data.get('fc_jam', {}))
                ]
                
                # Insert absensi
                last_no = DatabaseConnector.execute_raw_sql(
                    insert_query, params, using=db_name, return_last_id=True, p_queri=False
                )

                # Insert Log
                log_query = "INSERT INTO db_log (fd_datetime, fv_username, fc_action, fv_log) VALUES (%s, %s, %s, %s)"
                DatabaseConnector.execute_raw_sql(
                    log_query, [now, username, 'create', f"absensi_dps => fn_no: {last_no}"], using=db_name
                )

                # 5. LOGIKA DRY-RUN
                if is_dryrun:
                    # Rollback instruksi transaksi yang aktif (pada database db_name)
                    transaction.set_rollback(True, using=db_name)
                    
                    return JsonResponse({
                        'status': 'ok',
                        'message': 'Dry-run berhasil. Query valid dan dieksekusi, tetapi data DIBATALKAN (rollback).',
                        'debug_data': {
                            'simulated_fn_no': last_no,
                            'params_used': params
                        }
                    }, status=200)

            # 6. COMMIT NORMAL (Jika bukan dry-run)
            return JsonResponse({'status': 'ok', 'fn_no': last_no}, status=201)
            
        except Exception as e:
            # Gunakan self._catch_error_details sesuai gaya kode Anda sebelumnya
            return self._catch_error_details(e)

    def post_report(self, request, data):
        """Handler untuk Mengambil Data Report (View, Excel, PDF)"""
        if not data.get('date1') or not data.get('date2'):
            return JsonResponse({
                "status": "error",
                "message": "Parameter date1 dan date2 wajib diisi."
            }, status=400)
            
        level = data.get('level', 'spg')
        action = data.get('action', 'view')
        
        # Panggil Helper pemrosesan Pandas (Pastikan fungsi ini sudah Anda deklarasikan di view)
        report_data = self.get_attendance_data(data, level, action)
        
        if action == 'view':
            return JsonResponse({
                "status": "success", "type": "datatable", "level": level,
                "data": report_data['detailAbsen']
            }, status=200)
            
        elif action == 'excel':
            return JsonResponse({
                "status": "success", "type": "excel_rekap", "level": level,
                "filename": f"data-absensi-{level}__{data['date1']}_{data['date2']}",
                "data": report_data
            }, status=200)
            
        elif action == 'pdf':
            return JsonResponse({
                "status": "success", "type": "pdf_view", "level": level,
                "data": report_data['detailAbsen']
            }, status=200)
            
        else:
            return JsonResponse({
                "status": "error", "message": f"Action '{action}' tidak dikenali."
            }, status=400)

    def post_update(self, request, data, username):
        """Handler untuk Update Absensi (Versi Teroptimasi)"""
        
        # Validasi ID Absensi
        fn_no = data.get('fn_no')
        if not fn_no:
            return JsonResponse({'status': 'error', 'message': 'Parameter fn_no diperlukan untuk update'}, status=400)

        # Parsing parameter dryrun
        is_dryrun = str(data.get('dryrun', False)).lower() in ['true', '1']
        
        # Pastikan dbPRoll sudah terdefinisi
        db_name = dbPRoll  

        try:
            with transaction.atomic(using=db_name):
                
                # 1. Ambil data lama (khususnya foto) agar tidak hilang saat update
                get_foto_query = "SELECT fc_foto FROM absensi_dps WHERE fn_no = %s"
                row = DatabaseConnector.execute_raw_sql(
                    get_foto_query, [fn_no], using=db_name, fetch_one=True
                )
                
                if not row:
                    return JsonResponse({'status': 'error', 'message': f'Data absensi dengan fn_no {fn_no} tidak ditemukan'}, status=404)
                
                # Ambil foto lama (gunakan default dict jika None)
                existing_foto = row.get('fc_foto') or json.dumps({'in': '', 'out': ''})

                # 2. Ekstraksi Dasar
                fc_jenis = data.get('fc_jenis', '')
                is_absen = (fc_jenis == 'absen')
                
                # 3. Transformasi Data
                fc_status = 'out' if is_absen else fc_jenis
                
                fc_tempat_raw = data.get('fc_tempat', {})
                fc_tempat = fc_tempat_raw if is_absen else {'in': fc_jenis, 'out': fc_jenis}
                
                fn_toko_in = data.get('fn_in', 0) if is_absen else 0
                fn_toko_out = data.get('fn_out', 0) if is_absen else 0
                
                # JSON default untuk fc_posisi dan fc_foto (jika bukan absen)
                default_dict = {'in': '', 'out': ''} if is_absen else {'in': fc_jenis, 'out': fc_jenis}
                default_json_str = json.dumps(default_dict)
                
                # Logika penentuan foto
                fc_foto = default_json_str if not is_absen else existing_foto
                
                # Timestamp creation
                now = datetime.now()
                fd_update_raw = data.get('fd_update')
                fd_update = datetime.fromisoformat(fd_update_raw) if is_absen and fd_update_raw else now

                # 4. Eksekusi Update
                update_query = """
                    UPDATE absensi_dps SET
                        fc_jenis = %s, fc_status = %s, fc_tempat = %s,
                        fn_toko_in = %s, fn_toko_out = %s, fc_shift = %s,
                        fc_posisi = %s, fc_foto = %s, fd_update = %s, 
                        stat = %s, fc_jam = %s
                    WHERE fn_no = %s
                """
                params = [
                    fc_jenis, 
                    fc_status, 
                    json.dumps(fc_tempat), 
                    fn_toko_in, 
                    fn_toko_out, 
                    json.dumps(data.get('fc_shift', {})),
                    default_json_str,  # fc_posisi
                    fc_foto,           # fc_foto (bisa dari data lama atau default)
                    fd_update, 
                    'u',
                    json.dumps(data.get('fc_jam', {})),
                    fn_no              # Parameter WHERE clause
                ]
                
                DatabaseConnector.execute_raw_sql(
                    update_query, params, using=db_name, p_queri=False
                )

                # Insert Log
                log_query = "INSERT INTO db_log (fd_datetime, fv_username, fc_action, fv_log) VALUES (%s, %s, %s, %s)"
                DatabaseConnector.execute_raw_sql(
                    log_query, [now, username, 'update', f"absensi_dps => fn_no: {fn_no}"], using=db_name
                )

                # 5. LOGIKA DRY-RUN
                if is_dryrun:
                    transaction.set_rollback(True, using=db_name)
                    
                    return JsonResponse({
                        'status': 'ok',
                        'message': 'Dry-run Update berhasil. Query valid dan dieksekusi, tetapi data DIBATALKAN (rollback).',
                        'debug_data': {
                            'target_fn_no': fn_no,
                            'params_used': params
                        }
                    }, status=200)

            # 6. COMMIT NORMAL (Jika bukan dry-run)
            return JsonResponse({'status': 'ok', 'fn_no': fn_no, 'message': 'Update berhasil'}, status=200)
            
        except Exception as e:
            return self._catch_error_details(e)

    def post_delete(self, request, data, username):
        """Handler untuk Menghapus Data Absensi (Versi Teroptimasi)"""
        
        # Validasi Parameter
        id_absensi = data.get('fn_no')
        if not id_absensi:
            return JsonResponse({'status': 'error', 'message': 'Parameter fn_no diperlukan untuk menghapus'}, status=400)
            
        # Parsing parameter dryrun
        is_dryrun = str(data.get('dryrun', False)).lower() in ['true', '1']
        
        # Pastikan dbPRoll sudah terdefinisi di scope class/file ini
        db_name = dbPRoll  

        try:
            # PENTING: Arahkan atomic ke database yang tepat agar set_rollback tidak gagal
            with transaction.atomic(using=db_name):
                
                # 1. Jalankan eksekusi DELETE
                delete_query = "DELETE FROM absensi_dps WHERE fn_no = %s"
                DatabaseConnector.execute_raw_sql(
                    delete_query, [id_absensi], using=db_name, p_queri=False
                )
                
                # 2. Simpan Log (gunakan datetime.now() alih-alih datetime.now())
                now = datetime.now()
                log_query = "INSERT INTO db_log (fd_datetime, fv_username, fc_action, fv_log) VALUES (%s, %s, %s, %s)"
                DatabaseConnector.execute_raw_sql(
                    log_query, [now, username, 'delete', f"absensi_dps => fn_no: {id_absensi}"], using=db_name
                )
                
                # 3. LOGIKA DRY-RUN
                if is_dryrun:
                    transaction.set_rollback(True, using=db_name)
                    return JsonResponse({
                        'status': 'ok',
                        'message': 'Dry-run Delete berhasil. Query valid dan dieksekusi, tetapi data DIBATALKAN (rollback).',
                        'debug_data': {
                            'target_fn_no': id_absensi
                        }
                    }, status=200)        
                    
            # 4. COMMIT NORMAL
            return JsonResponse({
                'status': 'ok', 
                'message': f'Data dengan fn_no {id_absensi} berhasil dihapus'
            }, status=200)
            
        except Exception as e:
            # Gunakan helper error catch yang konsisten dengan metode lain
            return self._catch_error_details(e)
    def approval_abs(self, request, decoded_payload):
        """
        Endpoint untuk membuat pengajuan Approval Absen baru.
        """
        try:
            data = request.data
            # Ambil actor/user ID dari decoded payload JWT atau body request
            actor_id = decoded_payload.get('userid') or data.get('userid_actor') or data.get('fn_userid')

            # 1. Validasi Field Wajib (NOT NULL di skema DB)
            required_fields = [
                'fn_userid', 'fd_tanggal', 'fd_jam_in', 
                'fd_jam_out', 'fv_tempat_in', 'fv_tempat_out'
            ]
            missing_fields = [field for field in required_fields if not data.get(field)]
            
            if missing_fields:
                return Response({
                    "status": False,
                    "message": f"Field wajib tidak boleh kosong: {', '.join(missing_fields)}",
                    "data": {}
                }, status=status.HTTP_400_BAD_REQUEST)

            # 2. Parsing & Sanitasi Data Input
            fn_userid = int(data.get('fn_userid'))
            fd_tanggal = data.get('fd_tanggal')  # Format: YYYY-MM-DD
            fd_jam_in = data.get('fd_jam_in')     # Format: HH:MM:SS
            fd_jam_out = data.get('fd_jam_out')   # Format: HH:MM:SS
            fv_tempat_in = str(data.get('fv_tempat_in')).strip()
            fv_tempat_out = str(data.get('fv_tempat_out')).strip()
            
            # Field Opsional / Default
            fv_shfit = data.get('fv_shfit', '').strip() or None
            fv_note = data.get('fv_note', '').strip() or None
            fc_approve = data.get('fc_approve', 'W').upper()  # Default 'W' (Waiting)
            
            current_time = datetime.now()

            # 3. Eksekusi Save ke Database (Gunakan database router / using jika multi-db)
            
            with transaction.atomic(using=dbPRoll):
                approval_obj = ApprovalAbs.objects.using(dbPRoll).create(
                    fn_userid=fn_userid,
                    fd_tanggal=fd_tanggal,
                    fv_shfit=fv_shfit,
                    fd_jam_in=fd_jam_in,
                    fd_jam_out=fd_jam_out,
                    fv_tempat_in=fv_tempat_in,
                    fv_tempat_out=fv_tempat_out,
                    fv_note=fv_note,
                    fc_approve=fc_approve,
                    fn_userid_in=actor_id,
                    fn_userid_up=actor_id,
                    fd_create=current_time,
                    fd_update=current_time
                )

            return Response({
                "status": True,
                "message": "Pengajuan approval absen berhasil disimpan.",
                "data": {
                    "fn_id": approval_obj.fn_id,
                    "fn_userid": approval_obj.fn_userid,
                    "fd_tanggal": approval_obj.fd_tanggal,
                    "fc_approve": approval_obj.fc_approve
                }
            }, status=status.HTTP_201_CREATED)

        except ValueError as ve:
            return Response({
                "status": False,
                "message": f"Format data tidak valid: {str(ve)}",
                "data": {}
            }, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
                    raise e
class MHariView(APIView):
    
    def json_serializer(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        raise TypeError(f"Type {type(obj)} not serializable")

    # =========================================================================
    # 1. GET: Mengambil Semua Data
    # =========================================================================
    def get(self, request):
        auth_header = request.headers.get('Authorization')
        if not auth_header:
            return JsonResponse({"message": "Akses ditolak, token tidak ditemukan!"}), 401
        token = auth_header.split(" ")[1].strip()
        try:
            decoded_payload = jwt.decode(token, settings.SECRET_KEY, algorithms=["HS256"])
        except InvalidSignatureError:
            # Kembalikan respon 401 Unauthorized alih-alih membiarkan server error 500
            return JsonResponse({'error': 'Token tidak valid / Signature failed'}, status=401)
        except ExpiredSignatureError:
            return JsonResponse({'error': 'Token sudah kedaluwarsa'}, status=401)
        try:
            type_param = request.GET.get('type', 'data')
            if type_param == 'data_hbes':
                return self.get_haribesar_data(request,decoded_payload)
            else:
                return JsonResponse({'error': f"Type '{type_param}' tidak ditemukan"}, status=400)
        except Exception as e:
            return _catch_error_details(e)

    # =========================================================================
    # 2. POST: Mengarahkan Jalur Request (Routing Type)
    # =========================================================================
    def post(self, request):
        auth_header = request.headers.get('Authorization')
        if not auth_header:
            # 🛠️ PERBAIKAN 1: Gunakan DRF Response dan bungkus status code di dalam fungsi status=...
            return Response({"message": "Akses ditolak, token tidak ditemukan!"}, status=status.HTTP_401_UNAUTHORIZED)
            
        token = auth_header.split(" ")[1].strip()
        try:
            decoded_payload = jwt.decode(token, settings.SECRET_KEY, algorithms=["HS256"])
        except InvalidSignatureError:
            # 🛠️ PERBAIKAN 2: Ubah ke DRF Response agar konsisten
            return Response({'error': 'Token tidak valid / Signature failed'}, status=status.HTTP_401_UNAUTHORIZED)
        except ExpiredSignatureError:
            # 🛠️ PERBAIKAN 3: Ubah ke DRF Response agar konsisten
            return Response({'error': 'Token sudah kedaluwarsa'}, status=status.HTTP_401_UNAUTHORIZED)
            
        # Ambil 'type' dari body request JSON
        type_param = request.data.get('type')        
        try:
            if type_param == 'upsert_hbes':
                return self.upsert_hbes(request, decoded_payload)
                
            # 🛠️ PERBAIKAN 4: Ubah ke DRF Response agar konsisten
            return Response({
                'success': False, 
                'message': f"Method type '{type_param}' tidak valid atau tidak ditemukan."
            }, status=status.HTTP_400_BAD_REQUEST)
            
        except Exception as e:
            return _catch_error_details(e)

    # =========================================================================
    # 3. GET DATA ACTION
    # =========================================================================
    def get_haribesar_data(self, request,decoded_payload):
        query = "SELECT * FROM m_hari_besar ORDER BY fd_tanggal DESC"
        raw_hbes = DatabaseConnector.execute_raw_sql(query, using='payroll', p_queri=False)
        df_hbes = pd.DataFrame(raw_hbes)
        
        return JsonResponse({
            "data_hari_besar": df_hbes.replace({np.nan: None}).to_dict(orient='records')
        }, safe=False)
    
    # =========================================================================
    # 4. UPSERT ACTION (INSERT & UPDATE NATIVE SQL)
    # =========================================================================
    def upsert_hbes(self, request,decoded_payload):
        data = request.data
        user_id = request.data.get('userid',decoded_payload.get('userid'))
        # Cek apakah ini mode UPDATE (ada fn_id) atau INSERT (tidak ada fn_id)
        fn_id = data.get('fn_id', None)
        now = datetime.now()
        
        # ----------------- KONDISI 1: UPDATE DATA -----------------
        if fn_id:
            # Validasi minimal untuk update
            required_fields = ['fd_tanggal', 'fv_nama_libur']
            for field in required_fields:
                if field not in data or data[field] is None:
                    return Response({"status": "error", "message": f"Field '{field}' wajib diisi untuk update."}, status=status.HTTP_400_BAD_REQUEST)
            
            query = """
                UPDATE m_hari_besar 
                SET fd_tanggal = %s, 
                    fv_nama_libur = %s, 
                    fv_keterangan = %s, 
                    fc_hold = %s, 
                    fd_updated_at = %s, 
                    fn_user_update = %s
                WHERE fn_id = %s
            """
            params = (
                data['fd_tanggal'], 
                data['fv_nama_libur'], 
                data.get('fv_keterangan', None), 
                data.get('fc_hold', '0'), 
                now, 
                user_id,
                fn_id
            )
            
            # Eksekusi raw sql update dengan passing tuple params
            DatabaseConnector.execute_raw_sql(query, params=params, using='payroll', p_queri=False)
            
            return Response({
                "status": "success",
                "message": f"Data hari besar dengan ID {fn_id} berhasil diperbarui",
                "data": {"fn_id": fn_id, "fv_nama_libur": data['fv_nama_libur']}
            }, status=status.HTTP_200_OK)
            
        # ----------------- KONDISI 2: INSERT DATA -----------------
        else:
            required_fields = ['fd_tanggal', 'fv_nama_libur']
            for field in required_fields:
                if field not in data or data[field] is None:
                    return Response({"status": "error", "message": f"Field '{field}' wajib diisi untuk data baru."}, status=status.HTTP_400_BAD_REQUEST)
            
            query = """
                INSERT INTO m_hari_besar 
                (fd_tanggal, fv_nama_libur, fv_keterangan, fc_hold, fd_created_at, fn_user_create) 
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            params = (
                data['fd_tanggal'], 
                data['fv_nama_libur'], 
                data.get('fv_keterangan', None), 
                data.get('fc_hold', '0'), 
                now, 
                user_id
            )
            
            # Eksekusi raw sql insert dengan passing tuple params
            # Catatan: Jika DatabaseConnector Anda mengembalikan lastrowid, tangkap nilainya di sini.
            # Contoh jika me-return ID: inserted_id = DatabaseConnector.execute_raw_sql(...)
            inserted_id = DatabaseConnector.execute_raw_sql(query, params=params, using='payroll', p_queri=False)
                
            return Response({
                "status": "success",
                "message": "Data hari besar baru berhasil disimpan",
                "data": {
                    "fn_id": inserted_id if inserted_id else "Generated by DB",
                    "fd_tanggal": data['fd_tanggal'],
                    "fv_nama_libur": data['fv_nama_libur']
                }
            }, status=status.HTTP_201_CREATED)

class MKoordinatTokoView(DataLayerMixin,APIView):
    
    def json_serializer(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        raise TypeError(f"Type {type(obj)} not serializable")

    # =========================================================================
    # 1. GET: Mengarahkan Jalur Request Berdasarkan Type
    # =========================================================================
    def get(self, request):
        auth_header = request.headers.get('Authorization')
        if not auth_header:
            return JsonResponse({"message": "Akses ditolak, token tidak ditemukan!"}), 401
        token = auth_header.split(" ")[1].strip()
        try:
            decoded_payload = jwt.decode(token, settings.SECRET_KEY, algorithms=["HS256"])
        except InvalidSignatureError:
            # Kembalikan respon 401 Unauthorized alih-alih membiarkan server error 500
            return JsonResponse({'error': 'Token tidak valid / Signature failed'}, status=401)
        except ExpiredSignatureError:
            return JsonResponse({'error': 'Token sudah kedaluwarsa'}, status=401)
        try:
            type_param = request.GET.get('type', 'data')
            if type_param == 'data_koordinat':
                return self.get_koordinat_data(request,decoded_payload)
            else:
                return JsonResponse({'error': f"Type '{type_param}' tidak ditemukan"}, status=400)
        except Exception as e:
            return _catch_error_details(e)

    # =========================================================================
    # 2. POST: Mengarahkan Jalur Request (Routing Type)
    # =========================================================================
    def post(self, request):
        auth_header = request.headers.get('Authorization')
        if not auth_header:
            # 🛠️ PERBAIKAN 1: Gunakan DRF Response dan bungkus status code di dalam fungsi status=...
            return Response({"message": "Akses ditolak, token tidak ditemukan!"}, status=status.HTTP_401_UNAUTHORIZED)
            
        token = auth_header.split(" ")[1].strip()
        try:
            decoded_payload = jwt.decode(token, settings.SECRET_KEY, algorithms=["HS256"])
        except InvalidSignatureError:
            # 🛠️ PERBAIKAN 2: Ubah ke DRF Response agar konsisten
            return Response({'error': 'Token tidak valid / Signature failed'}, status=status.HTTP_401_UNAUTHORIZED)
        except ExpiredSignatureError:
            # 🛠️ PERBAIKAN 3: Ubah ke DRF Response agar konsisten
            return Response({'error': 'Token sudah kedaluwarsa'}, status=status.HTTP_401_UNAUTHORIZED)
        type_param = request.data.get('type')        
        try:
            if type_param == 'upsert_koordinat':
                return self.upsert_koordinat(request,decoded_payload)
                
            return JsonResponse({
                'success': False, 
                'message': f"Method type '{type_param}' tidak valid atau tidak ditemukan."
            }, status=400)
            
        except Exception as e:
            return _catch_error_details(e)

    # =========================================================================
    # 3. GET DATA ACTION
    # =========================================================================
    def get_koordinat_data(self, request,decoded_payload):
        # Mengambil semua data koordinat toko, diurutkan berdasarkan inputan terbaru
        ##
        query = """SELECT a.* FROM m_koordinat_toko a 
        ORDER BY fd_created_at DESC"""
        raw_data = DatabaseConnector.execute_raw_sql(query, using='payroll', p_queri=False)
        df_koordinat = pd.DataFrame(raw_data)
        df_custb=self.df_custb(colom='fn_cusid, fv_address')
        df_koordinat=df_koordinat.merge(df_custb,how='left',on='fn_cusid')
        ## Harus diganti ke 237 x
        return JsonResponse({
            "data_koordinat_toko": df_koordinat.replace({np.nan: None}).to_dict(orient='records')
        }, safe=False)
    
    # =========================================================================
    # 4. UPSERT ACTION (INSERT & UPDATE NATIVE SQL)
    # =========================================================================
    def upsert_koordinat(self, request,decoded_payload):
        data = request.data
        user_id = request.data.get('userid',decoded_payload.get('userid'))
        
        # Cek apakah ini mode UPDATE (ada fn_cusid) atau INSERT (tidak ada fn_cusid)
        fn_cusid = data.get('fn_cusid', None)
        now = datetime.now()
        
        # ----------------- KONDISI 1: UPDATE DATA -----------------
        if fn_cusid:
            # Validasi field wajib untuk proses update koordinat
            required_fields = ['fc_codecus', 'fv_namecus', 'fn_latitude', 'fn_longitude', 'fn_radius']
            for field in required_fields:
                if field not in data or data[field] is None:
                    return Response({"status": "error", "message": f"Field '{field}' wajib diisi untuk update."}, status=status.HTTP_400_BAD_REQUEST)
            
            query = """
                UPDATE m_koordinat_toko 
                SET fc_codecus = %s, 
                    fv_namecus = %s, 
                    fv_coordinate = %s, 
                    fn_latitude = %s, 
                    fn_longitude = %s, 
                    fn_radius = %s, 
                    fc_hold = %s, 
                    fd_updated_at = %s, 
                    fn_user_update = %s
                WHERE fn_cusid = %s
            """
            params = (
                data['fc_codecus'],
                data['fv_namecus'],
                data.get('fv_coordinate', None),
                data['fn_latitude'],
                data['fn_longitude'],
                data['fn_radius'],
                data.get('fc_hold', '0'),
                now,
                user_id,
                fn_cusid
            )
            
            # Eksekusi raw sql update
            DatabaseConnector.execute_raw_sql(query, params=params, using='payroll', p_queri=False)
            
            return Response({
                "status": "success",
                "message": f"Data koordinat toko dengan ID {fn_cusid} berhasil diperbarui",
                "data": {"fn_cusid": fn_cusid, "fv_namecus": data['fv_namecus']}
            }, status=status.HTTP_200_OK)
            
        # ----------------- KONDISI 2: INSERT DATA -----------------
        else:
            # Validasi field wajib untuk data baru
            required_fields = ['fc_codecus', 'fv_namecus', 'fn_latitude', 'fn_longitude', 'fn_radius']
            for field in required_fields:
                if field not in data or data[field] is None:
                    return Response({"status": "error", "message": f"Field '{field}' wajib diisi untuk data baru."}, status=status.HTTP_400_BAD_REQUEST)
            
            query = """
                INSERT INTO m_koordinat_toko 
                (fc_codecus, fv_namecus, fv_coordinate, fn_latitude, fn_longitude, fn_radius, fc_hold, fd_created_at, fn_user_create) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            params = (
                data['fc_codecus'],
                data['fv_namecus'],
                data.get('fv_coordinate', None),
                data['fn_latitude'],
                data['fn_longitude'],
                data['fn_radius'],
                data.get('fc_hold', '0'),
                now,
                user_id
            )
            
            # Eksekusi raw sql insert dan menangkap ID baru (jika DatabaseConnector mendukung return ID)
            inserted_id = DatabaseConnector.execute_raw_sql(query, params=params, using='payroll', p_queri=False)
                
            return Response({
                "status": "success",
                "message": "Data koordinat toko baru berhasil disimpan",
                "data": {
                    "fn_cusid": inserted_id if inserted_id else "Generated by DB",
                    "fc_codecus": data['fc_codecus'],
                    "fv_namecus": data['fv_namecus']
                }
            }, status=status.HTTP_201_CREATED)

class Submission(APIView):
    """
    Service class untuk menangani query dan logika persetujuan pengajuan (Submission).
    """
    def get(self, request):
        # auth_header = request.headers.get('Authorization')
        # if not auth_header:
        #     return JsonResponse({"message": "Akses ditolak, token tidak ditemukan!"}), 401
        # token = auth_header.split(" ")[1].strip()
        # try:
        #     decoded_payload = jwt.decode(token, settings.SECRET_KEY, algorithms=["HS256"])
        # except InvalidSignatureError:
        #     # Kembalikan respon 401 Unauthorized alih-alih membiarkan server error 500
        #     return JsonResponse({'error': 'Token tidak valid / Signature failed'}, status=401)
        # except ExpiredSignatureError:
        #     return JsonResponse({'error': 'Token sudah kedaluwarsa'}, status=401)
        try:
            type_param = request.GET.get('type', 'data')
            if type_param == 'data_submission':
                return self.get_submission(request)

            else:
                return JsonResponse({'error': f"Type '{type_param}' tidak ditemukan"}, status=400)
        except Exception as e:
            return _catch_error_details(e)
            
    def get_submission(self, request):
        start_date = request.GET.get('start')
        end_date = request.GET.get('end')
        data=request.GET.get('data')
        level=request.GET.get('level')
        action=request.GET.get('action')
        date1 = datetime.strptime(start_date, '%Y-%m-%d').date()
        date2 = datetime.strptime(end_date, '%Y-%m-%d').date()
        ####################################################################
        # 1. Penanganan Query Database
        if level == 'supervisor':
            raw_query = """
                SELECT
                    e.*,
                    ap.*
                FROM (
                    SELECT * FROM approval_dps_dev
                    WHERE DATE(fd_mulai) >= %s AND DATE(fd_mulai) <= %s
                ) ap
                JOIN (
                    SELECT fn_userid FROM user_tb WHERE fn_userlevel IN (2,12)
                ) u ON ap.fn_userid = u.fn_userid
                JOIN (
                    SELECT fn_userid, fv_nameemployees FROM employees_tb
                    WHERE fn_iddepart IN (8,20)
                ) e ON u.fn_userid = e.fn_userid
                ORDER BY ap.fd_mulai
            """
            params = [date1, date2]
        else:
            raw_query = """
                SELECT
                    c.*,
                    es.fv_nameemployees AS spv_nameemployees,
                    e.*,
                    ap.*
                FROM (
                    SELECT * FROM approval_dps_dev
                    WHERE DATE(fd_mulai) >= %s AND DATE(fd_mulai) <= %s
                ) ap
                JOIN (
                    SELECT fn_userid FROM user_tb WHERE fn_userlevel IN (1,8,13)
                ) u ON ap.fn_userid = u.fn_userid
                JOIN (
                    SELECT fn_userid, fv_nameemployees FROM employees_tb
                    WHERE fn_iddepart IN (8,20)
                ) e ON u.fn_userid = e.fn_userid
                JOIN (
                    SELECT fn_cusid, fn_userid FROM t_aksesusercust
                    GROUP BY fn_userid
                ) a ON u.fn_userid = a.fn_userid
                JOIN (
                    SELECT fn_cusid, fc_codecus, fv_namecus FROM customer_tb
                    WHERE fc_hold = 'F'
                ) c ON a.fn_cusid = c.fn_cusid
                JOIN (
                    SELECT fn_cusid, fn_userid FROM t_aksesusercust
                ) aus ON c.fn_cusid = aus.fn_cusid
                JOIN (
                    SELECT fn_userid FROM user_tb
                    WHERE fn_userlevel IN (2,12)
                ) us ON aus.fn_userid = us.fn_userid
                JOIN (
                    SELECT fn_userid, fv_nameemployees FROM employees_tb
                    WHERE fn_iddepart IN (8,20) AND fc_hold = 'F'
                ) es ON us.fn_userid = es.fn_userid
                ORDER BY ap.fd_mulai
            """
            params = [date1, date2]

        # # Eksekusi Raw SQL Query & mapping ke list of dicts
        # with connection.cursor() as cursor:
        #     cursor.execute(raw_query, params)
        #     columns = [col[0] for col[description]]
        #     result_data = [
        #         dict(zip(columns, row))
        #         for row in cursor.fetchall()
        #     ]
        result_data = DatabaseConnector.execute_raw_sql(raw_query,params, using='payroll',p_queri=False)
        ## Harus diganti ke 237 ##################################################################
        # print(result_data)
        filename = f"data-pengajuan-supervisor__{start_date}_{end_date}" if level == 'supervisor' else f"data-pengajuan-SPG__{start_date}_{end_date}"

        # 2. Penanganan Action (View / Excel)
        if action == 'view':
            # Mengembalikan list of dicts yang bisa langsung di-pass ke Response JSON (Datatables/Serializer)
            df = pd.DataFrame(result_data)
            return JsonResponse({
            "detailAbsen": df.replace({np.nan: None}).to_dict(orient='records'),
            # "rekapAbsen": rekap_list,
            # 'userlist':user_list
        }, safe=False)
    

        elif action == 'excel':
            # Menggunakan Pandas untuk export Excel (serupa Excel::create Laravel)
            df = pd.DataFrame(result_data)
            output = BytesIO()

            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                df.to_excel(writer, sheet_name='Sheet1', index=False)

            output.seek(0)
            response = HttpResponse(
                output.getvalue(),
                content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            )
            response['Content-Disposition'] = f'attachment; filename="{filename}.xlsx"'
            return response

        return None

    # @transaction.atomic
    # def approve_submission(self, request_data):
    #     """
    #     Catatan: request_data diharapkan berupa dict / object parsed dari request.
    #     Contoh: {'no': 123, 'type': 'approve', 'note': 'acc'}
    #     """
    #     no = request_data.get('no')
    #     req_type = request_data.get('type')
    #     note = request_data.get('note', '-')

    #     # 1. Update Status Approval
    #     fc_approve_val = 'Y' if req_type == 'approve' else 'N'
        
    #     # Jika menggunakan koneksi spesifik 'live', sesuaikan dengan .using('live')
    #     ApprovalDpsDev.objects.filter(fn_no=no).update(
    #         fc_approve=fc_approve_val,
    #         fd_update=timezone.now(),
    #         fv_note=note,
    #         stat='u'
    #     )

    #     # 2. Proses Approval
    #     if req_type == 'approve':
    #         pengajuan = ApprovalDpsDev.objects.filter(fn_no=no).first()

    #         if pengajuan:
    #             # Blok 1: Proses untuk Jenis Non-Lembur, Non-Tukar Off, Non-Other
    #             if pengajuan.fc_jenis not in ['lembur', 'tukar off', 'other']:
    #                 current_date = pengajuan.fd_mulai
    #                 end_date = pengajuan.fd_sampai

    #                 while current_date <= end_date:
    #                     status = 'off non-schedule' if pengajuan.fc_jenis == 'other' else pengajuan.fc_jenis
    #                     keterangan = pengajuan.fv_keterangan if pengajuan.fc_jenis == 'other' else pengajuan.fc_jenis
    #                     jenis = 'off' if pengajuan.fc_jenis == 'other' else pengajuan.fc_jenis

    #                     AbsensiDps.objects.create(
    #                         fn_userid=pengajuan.fn_userid,
    #                         fd_tanggal=current_date,
    #                         fc_status=status,
    #                         fc_jenis=jenis,
    #                         fc_tempat={'in': jenis, 'out': jenis},  # Di-serialize otomatis ke JSON oleh JSONField
    #                         fc_shift={'in': keterangan, 'out': keterangan},
    #                         fd_create=timezone.now(),
    #                         stat='i',
    #                         sync=0
    #                     )
    #                     current_date += timedelta(days=1)

    #             # Blok 2: Proses untuk Jenis Tukar Off atau Other
    #             if pengajuan.fc_jenis in ['tukar off', 'other']:
    #                 tgl = pengajuan.fd_sampai

    #                 AbsensiDps.objects.create(
    #                     fn_userid=pengajuan.fn_userid,
    #                     fd_tanggal=tgl,
    #                     fc_status='off',
    #                     fc_jenis='off',
    #                     fc_tempat={'in': 'off', 'out': 'off'},
    #                     fc_jam={'in': 'off', 'out': 'off'},
    #                     fc_foto={'in': 'off', 'out': 'off'},
    #                     fc_shift={'in': 'off', 'out': 'off'},
    #                     fn_toko_in=0,
    #                     fd_create=timezone.now(),
    #                     stat='i'
    #                 )

    #     # 3. Proses Cancellation (Batal)
    #     if req_type == 'cancel':
    #         pengajuan = ApprovalDpsDev.objects.filter(fn_no=no).first()

    #         if pengajuan and pengajuan.fc_jenis != 'lembur':
    #             current_date = pengajuan.fd_mulai
    #             end_date = pengajuan.fd_sampai

    #             while current_date <= end_date:
    #                 AbsensiDps.objects.filter(
    #                     fd_tanggal=current_date,
    #                     fc_jenis=pengajuan.fc_jenis,
    #                     fn_userid=pengajuan.fn_userid
    #                 ).delete()
    #                 current_date += timedelta(days=1)

    #     return 'ok'

class Cus_stay(DataLayerMixin,APIView):
    """
    Service class untuk menangani query dan logika persetujuan pengajuan (Submission).
    """
    def post(self, request):
        auth_header = request.headers.get('Authorization')
        if not auth_header:
            # 🛠️ PERBAIKAN 1: Gunakan DRF Response dan bungkus status code di dalam fungsi status=...
            return Response({"message": "Akses ditolak, token tidak ditemukan!"}, status=status.HTTP_401_UNAUTHORIZED)
            
        token = auth_header.split(" ")[1].strip()
        try:
            decoded_payload = jwt.decode(token, settings.SECRET_KEY, algorithms=["HS256"])
        except InvalidSignatureError:
            # 🛠️ PERBAIKAN 2: Ubah ke DRF Response agar konsisten
            return Response({'error': 'Token tidak valid / Signature failed'}, status=status.HTTP_401_UNAUTHORIZED)
        except ExpiredSignatureError:
            # 🛠️ PERBAIKAN 3: Ubah ke DRF Response agar konsisten
            return Response({'error': 'Token sudah kedaluwarsa'}, status=status.HTTP_401_UNAUTHORIZED)
            
        # Ambil 'type' dari body request JSON
        type_param = request.data.get('type')        
        try:
            if type_param == 'upsert_cus_stay':
                return self.upsert_cus_stay(request, decoded_payload)
                
            # 🛠️ PERBAIKAN 4: Ubah ke DRF Response agar konsisten
            return Response({
                'success': False, 
                'message': f"Method type '{type_param}' tidak valid atau tidak ditemukan."
            }, status=status.HTTP_400_BAD_REQUEST)
            
        except Exception as e:
            return _catch_error_details(e)
        
    def get(self, request):
        # auth_header = request.headers.get('Authorization')
        # if not auth_header:
        #     return JsonResponse({"message": "Akses ditolak, token tidak ditemukan!"}), 401
        # token = auth_header.split(" ")[1].strip()
        # try:
        #     decoded_payload = jwt.decode(token, settings.SECRET_KEY, algorithms=["HS256"])
        # except InvalidSignatureError:
        #     # Kembalikan respon 401 Unauthorized alih-alih membiarkan server error 500
        #     return JsonResponse({'error': 'Token tidak valid / Signature failed'}, status=401)
        # except ExpiredSignatureError:
        #     return JsonResponse({'error': 'Token sudah kedaluwarsa'}, status=401)
        try:
            type_param = request.GET.get('type', 'data')
            if type_param == 'data_cust_stay':
                return self.get_cus_stay(request)
            else:
                return JsonResponse({'error': f"Type '{type_param}' tidak ditemukan"}, status=400)
        except Exception as e:
            return _catch_error_details(e)
            
    def get_cus_stay(self, request):
        # data=request.GET.get('data')
        # level=request.GET.get('level')
        # date1 = datetime.strptime(start_date, '%Y-%m-%d').date()
        # date2 = datetime.strptime(end_date, '%Y-%m-%d').date()
        df_cus_stay=self.df_cus_stay_(colom=("fn_userid,fn_cusid,fc_up_one"))
        list_user=list_to_sql_in(df_cus_stay,"fn_userid")
        list_cus=list_to_sql_in(df_cus_stay,"fn_cusid")
        # print(list_user)
        filters={"fn_userid__in":[list_user[1:-1]]}
        filtercus={"fn_cusid__in":[list_cus[1:-1]]}
        df_emp=self.df_employee_sl(colom=("fn_userid,fv_nameemployees"),queri=False, **filters)
        df_cus=self.df_cust_(colom=("fn_cusid,fv_toko"), **filtercus)
        df_user=self.df_user_lv_(colom=("u.fn_userid,fv_level"), **filters)
        df_cus_stay=df_cus_stay.merge(df_emp,how='left',on='fn_userid')
        df_cus_stay=df_cus_stay.merge(df_user,how='left',on='fn_userid')
        df_cus_stay=df_cus_stay.merge(df_cus,how='left',on='fn_cusid')
        df_cus_stay=df_cus_stay.replace({"operator":"SPG-B"})
        return JsonResponse({
        "cus_stay": df_cus_stay.replace({np.nan: None}).to_dict(orient='records'),
        }, safe=False)

    def upsert_cus_stay(self, request,decoded_payload):
        data = request.data
        print(data)
        user_id = request.data.get('userid',decoded_payload.get('userid'))
        user_spv=data.get('fn_userid',None)
        # Cek apakah ini mode UPDATE (ada fn_id) atau INSERT (tidak ada fn_id)
        fn_cusid = data.get('fn_cusid', None)
        now = datetime.now()
        
        print(user_id,user_spv,fn_cusid)
        
        # ----------------- KONDISI 1: UPDATE DATA -----------------
        if fn_cusid:
            # Validasi minimal untuk update
            required_fields = ['fn_userid', 'fn_cusid']
            for field in required_fields:
                if field not in data or data[field] is None:
                    return Response({"status": "error", "message": f"Field '{field}' wajib diisi untuk update."}, status=status.HTTP_400_BAD_REQUEST)
            
            query = """
                UPDATE m_cus_stay 
                SET fn_cusid = %s, 
                    fd_updated_at = %s, 
                    fn_userid_up = %s
                WHERE fn_userid = %s
            """
            params = (
                fn_cusid,
                now, 
                user_id,
                user_spv
            )
            
            # Eksekusi raw sql update dengan passing tuple params
            DatabaseConnector.execute_raw_sql(query, params=params, using='payroll', p_queri=False)
            
            return Response({
                "status": "success",
                "message": f"Data hari besar dengan ID {user_spv} berhasil diperbarui",
                "data": {"fn_id": user_spv, "fn_cusid (stay)": fn_cusid}
            }, status=status.HTTP_200_OK)
            
        # ----------------- KONDISI 2: INSERT DATA -----------------
        else:
            required_fields = ['fn_userid', 'fn_cusid']
            for field in required_fields:
                if field not in data or data[field] is None:
                    return Response({"status": "error", "message": f"Field '{field}' wajib diisi untuk data baru."}, status=status.HTTP_400_BAD_REQUEST)
            ### JIKA PERLU INSERT
            # query = """
            #     INSERT INTO m_hari_besar 
            #     (fd_tanggal, fv_nama_libur, fv_keterangan, fc_hold, fd_created_at, fn_user_create) 
            #     VALUES (%s, %s, %s, %s, %s, %s)
            # """
            # params = (
            #     data['fd_tanggal'], 
            #     data['fv_nama_libur'], 
            #     data.get('fv_keterangan', None), 
            #     data.get('fc_hold', '0'), 
            #     now, 
            #     user_id
            # )
            
            # # Eksekusi raw sql insert dengan passing tuple params
            # # Catatan: Jika DatabaseConnector Anda mengembalikan lastrowid, tangkap nilainya di sini.
            # # Contoh jika me-return ID: inserted_id = DatabaseConnector.execute_raw_sql(...)
            # inserted_id = DatabaseConnector.execute_raw_sql(query, params=params, using='payroll', p_queri=False)
                
            return Response({
                "status": "success",
                "message": "Data hari besar baru berhasil disimpan",
                # "data": {
                #     "fn_id": inserted_id if inserted_id else "Generated by DB",
                #     "fd_tanggal": data['fd_tanggal'],
                #     "fv_nama_libur": data['fv_nama_libur']
                # }
            }, status=status.HTTP_201_CREATED)
class HirarkiMarketing(DataLayerMixin,APIView):
    """
    Service class untuk menangani query dan logika persetujuan pengajuan (Submission).
    """
    def get(self, request):
        # auth_header = request.headers.get('Authorization')
        # if not auth_header:
        #     return JsonResponse({"message": "Akses ditolak, token tidak ditemukan!"}), 401
        # token = auth_header.split(" ")[1].strip()
        # try:
        #     decoded_payload = jwt.decode(token, settings.SECRET_KEY, algorithms=["HS256"])
        # except InvalidSignatureError:
        #     # Kembalikan respon 401 Unauthorized alih-alih membiarkan server error 500
        #     return JsonResponse({'error': 'Token tidak valid / Signature failed'}, status=401)
        # except ExpiredSignatureError:
        #     return JsonResponse({'error': 'Token sudah kedaluwarsa'}, status=401)
        try:
            type_param = request.GET.get('type', 'data')
            if type_param == 'data_hirarki':
                return self.get_hirarki(request)
            else:
                return JsonResponse({'error': f"Type '{type_param}' tidak ditemukan"}, status=400)
        except Exception as e:
            return _catch_error_details(e)
            
    def get_hirarki(self, request):
        # data=request.GET.get('data')
        # level=request.GET.get('level')
        # date1 = datetime.strptime(start_date, '%Y-%m-%d').date()
        # date2 = datetime.strptime(end_date, '%Y-%m-%d').date()
        filters={"emp__fn_iddepart__in":[8, 20],
                 "u__fn_userlevel__in": [2,12],
                "emp__fc_hold": "F",
                "u__fc_hold": "F",}
        
        user_spv=request.GET.get('userid')
        if user_spv:
            user_add = {"u__fn_userid": int(user_spv)}
            filters.update(user_add)

        df_user=self.df_employee_(colom="u.fn_userid",**filters)
        list_user=list_to_sql_in(df_user,"fn_userid")
        df_toko_hendle=self.df_toko_handle_(
            ts__fn_userid__in=[list_user[1:-1]], 
            queri=False
        )
        # filters={"emp__fn_iddepart__in":[8, 20],
        #          "u__fn_userlevel__in": [4],
        #         "emp__fc_hold": "F",
        #         "u__fc_hold": "F",}
        # df_manager=self.df_employee_(colom="u.fn_userid user_manager, emp.fv_nameemployees manager",**filters)
        # print(df_manager)
        # list_manager=list_to_sql_in(df_manager,"user_manager")
        # df_cus_stay=self.df_cus_stay_(colom=("fn_userid user_manager,fn_cusid"),fn_userid__in=[list_manager[1:-1]])
        # df_cus_stay=df_cus_stay.merge(df_toko_hendle[['fn_cusid','spv']],how='inner',on='fn_cusid')
        # print(df_cus_stay)
        # df_toko_hendle=df_toko_hendle.merge(df_cus_stay[['spv','user_manager']],how='left',on='spv')
        # df_toko_hendle=df_toko_hendle.merge(df_manager,how='left',on='user_manager')
        # 2. Hardcode pemetaan (Mapping) SPV -> Manager
        spv_to_manager = {
            'Faisal Anshar Henas': 'Budiono',
            'Nina Meiriyana': 'Budiono',
            'Yuyun Yuningsih': 'Budiono',
            'Desi Ari Herawatie': 'Budiono',
            'Oggi Maulana Fajar': 'Jason Lee',
            'Rohyati': 'Jason Lee',
            'Dewi Pujiyani': 'Jason Lee',
            'Sri Rahayu': 'Jason Lee',
            'MULYADI': 'Jason Lee',
            'CICILIA MANCA KALAMBE': 'Jerry Thio, Joe,SE',
            'Imam Hanafi': 'Jerry Thio, Joe,SE',
            'Mariyam': 'Jerry Thio, Joe,SE',
            'Setiyowati': 'Jerry Thio, Joe,SE',
            'Alfian Eka Nugroho S H': 'Tjwa, Agustinus Elsan',
        }
        emp_manager={"Budiono":"ASM-0005",
                     "Jason Lee":"ASM-0003",
                     "Jerry Thio, Joe,SE":"ASM-0002",
                     "Tjwa, Agustinus Elsan":"ASM-0001",}

        # 3. Buat kolom baru 'manager'
        df_toko_hendle['manager'] = df_toko_hendle['spv'].map(spv_to_manager)
        df_toko_hendle['emp_manager'] = df_toko_hendle['manager'].map(emp_manager)
        df_toko_hendle=df_toko_hendle.rename(
                            columns={
                                'fn_userid': 'spg_id',
                                'fv_nama': 'spg',
                            }
                        )
        # list_user=list_to_sql_in(df_cus_stay,"fn_userid")
        # list_cus=list_to_sql_in(df_cus_stay,"fn_cusid")
        # # print(list_user)
        # filters={"fn_userid__in":[list_user[1:-1]]}
        # filtercus={"fn_cusid__in":[list_cus[1:-1]]}
        # df_user=df_employee_sl(colom=("fn_userid,fv_nameemployees"),queri=False, **filters)
        # df_cus=df_cust_(colom=("fn_cusid,fv_toko"), **filtercus)
        # df_cus_stay=df_cus_stay.merge(df_user,how='left',on='fn_userid')
        # df_cus_stay=df_cus_stay.merge(df_cus,how='left',on='fn_cusid')
        return JsonResponse({
            # 'tets':df_cus_stay.replace({np.nan: None}).to_dict(orient='records'),
        "hirarki": df_toko_hendle.replace({np.nan: None}).to_dict(orient='records'),
        }, safe=False)
# -------------------------------------------------------------------------
# 1. ATTENDANCE API VIEW (Penyatuan GET, POST, PUT, DELETE)
# -------------------------------------------------------------------------
@method_decorator(csrf_exempt, name='dispatch')
class AttendanceAPIView(View):
    
    def get(self, request, level, no=None):
        """Menangani pengambilan data absensi (Single Detail atau List Data)"""
        try:
            # 1. Jika parameter 'no' dikirim, return detail absensi (JSON)
            if no is not None:
                # Mock up helper: detail = Attendance.get_attendance_detail(no)
                detail = {"fn_no": no, "info": "Detail Absensi"} 
                return JsonResponse(detail)

            # 2. Jika ada query parameter 'action', return data absensi untuk datatable/list
            action = request.GET.get('action')
            if action:
                # Merespon data berdasarkan request.GET, level, dan action
                # return JsonResponse(Attendance.get_attendance(request.GET, level, action), safe=False)
                return JsonResponse({"success": True, "data": []})
            print('test')
            # 3. Default: Ambil data Supervisor & Store untuk halaman render HTML
            staff_levels = [2, 12]
            ##
            query_supervisor = """
                SELECT et.fn_userid, et.fv_nameemployees 
                FROM employees_tb et
                INNER JOIN user_tb ut ON et.fn_userid = ut.fn_userid
                WHERE et.fn_iddepart IN (8, 20)
                  AND ut.fn_userlevel IN (%s, %s)
                  AND et.fc_hold = 'F'
                ORDER BY et.fv_nameemployees ASC
            """
            # Contoh eksekusi menggunakan DatabaseConnector Anda
            filter_by_supervisor = DatabaseConnector.execute_raw_sql(
                query=query_supervisor, params=staff_levels, using='payroll', fetch_all=True
            )
            ## Harus diganti ke 237 ##################################################################
            data = {
                'level': level,
                'filterBySupervisor': filter_by_supervisor,
                'store': []
            }

            if level == 'operator':
                query_store = "SELECT fn_cusid, fc_codecus, fv_namecus FROM customer_tb WHERE fc_hold = 'F'"
                data['store'] = DatabaseConnector.execute_raw_sql(query=query_store, using='local', fetch_all=True)

            # Jika request AJAX, kembalikan JSON, jika tidak return HTML render
            if request.headers.get('x-requested-with') == 'XMLHttpRequest':
                return JsonResponse(data)
                
            return render(request, f'dpspayrol/sales/work/attendance_{level}.html', data)

        except Exception as e:
            return JsonResponse({'success': False, 'error': str(e)}, status=500)

    def post(self, request, level, no=None):
        """Menangani pembuatan data absensi baru (Create)"""

        try:
            body = json.loads(request.body) if request.body else {}
            # create_data = Attendance.create_attendance(body)
            return JsonResponse({'success': True, 'message': 'Attendance created successfully'})
        except Exception as e:
            return JsonResponse({'success': False, 'error': str(e)}, status=500)
        
    def put(self, request, level, no=None):
        """Menangani pembaruan data absensi (Update)"""
        try:
            body = json.loads(request.body) if request.body else {}
            # update_data = Attendance.update_attendance(body)
            return JsonResponse({'success': True, 'message': 'Attendance updated successfully'})
        except Exception as e:
            return JsonResponse({'success': False, 'error': str(e)}, status=500)

    def delete(self, request, level, no=None):
        """Menangani penghapusan data absensi (Delete)"""
        try:
            body = json.loads(request.body) if request.body else {}
            # delete_data = Attendance.delete_attendance(body)
            return JsonResponse({'success': True, 'message': 'Attendance deleted successfully'})
        except Exception as e:
            return JsonResponse({'success': False, 'error': str(e)}, status=500)


# -------------------------------------------------------------------------
# 2. SUBMISSION API VIEW
# -------------------------------------------------------------------------
@method_decorator(csrf_exempt, name='dispatch')
class SubmissionAPIView(View):
    
    def get(self, request, level):
        """Menangani penampilan view atau penarikan data submission"""
        action = request.GET.get('action')
        if action:
            # return JsonResponse(Submission.get_submission(request.GET, level, action), safe=False)
            return JsonResponse({"success": True, "data": []})
            
        return render(request, f'dpspayrol/sales/work/submission_{level}.html', {'level': level})

    def post(self, request, level):
        """Menangani approval submission"""
        try:
            body = json.loads(request.body) if request.body else {}
            # return JsonResponse(Submission.approve_submission(body))
            return JsonResponse({"success": True, "message": "Submission approved"})
        except Exception as e:
            return JsonResponse({'success': False, 'error': str(e)}, status=500)


# -------------------------------------------------------------------------
# 3. EMPLOYEE WORK API VIEW (Gabungan Fitur Pendukung: Holiday, Sale, Claim, dll)
# -------------------------------------------------------------------------
@method_decorator(csrf_exempt, name='dispatch')
class EmployeeWorkAPIView(View):
    """Menangani per-routingan sub-fitur menggunakan parameter query seperti ?type=holiday"""
    def get(self, request):
        type_param = request.GET.get('type', 'holiday')
        is_ajax = request.headers.get('x-requested-with') == 'XMLHttpRequest'

        if type_param == 'holiday':
            if is_ajax:
                # return JsonResponse(Holiday.get_holiday(), safe=False)
                return JsonResponse({"holiday_data": []})
            return render(request, 'dpspayrol/sales/work/holiday.html')

        elif type_param == 'sale':
            data_param = request.GET.get('data')
            if data_param or is_ajax:
                # return JsonResponse(Employee.get_sale_info(request.GET))
                return JsonResponse({"sale_info": []})
            return render(request, 'dpspayrol/sales/work/sale.html')

        elif type_param == 'claim':
            emp = request.GET.get('emp')
            if emp or is_ajax:
                # return JsonResponse(Employee.get_claim_info(emp))
                return JsonResponse({"claim_info": []})
            return render(request, 'dpspayrol/sales/work/claim.html')

        elif type_param == 'saving':
            emp = request.GET.get('emp')
            if emp:
                # return JsonResponse(Employee.get_saving_info(emp))
                return JsonResponse({"saving_info": []})
            return JsonResponse({"message": "Employee parameter missing"}, status=400)

        elif type_param == 'overtime':
            emp = request.GET.get('emp')
            month = request.GET.get('month')
            if emp:
                # return JsonResponse(Employee.get_overtime_info(emp, month))
                return JsonResponse({"overtime_info": []})
            return JsonResponse({"message": "Employee parameter missing"}, status=400)

        return JsonResponse({'error': f"Type '{type_param}' tidak ditemukan"}, status=400)

    def post(self, request):
        """Menangani POST khusus untuk setHoliday atau data submission lainnya"""
        type_param = request.GET.get('type', 'holiday')
        try:
            body = json.loads(request.body) if request.body else {}
            
            if type_param == 'holiday':
                # return JsonResponse(Holiday.set_holiday(body))
                return JsonResponse({"success": True, "message": "Holiday successfully set"})
                
            return JsonResponse({'error': f"Action POST untuk type '{type_param}' tidak ditemukan"}, status=400)
        except Exception as e:
            return JsonResponse({'success': False, 'error': str(e)}, status=500)

class EmployeeController(APIView):
    
    def get(self, request):
        """
        Mengambil detail absensi (Pengganti getAttendanceDetail)
        """
        try:
            type_param = request.GET.get('type', 'spg')
            print(type_param)
            if type_param == 'spg':
                return self.get_ah(request, no=None)
            elif type_param == 'data':
                return self.get_attendance_data(request)
            else:
                return JsonResponse({'error': f"Type '{type_param}' tidak ditemukan"}, status=400)

        except Exception as e:
            return self._catch_error_details(e)
        
    def get_ah(self,request):
        employee_id=request.GET.get('userid', '')
        
        level = request.GET.get('level')
        # action = request.GET.get('action', '')
        # start_date = request.GET.get('start_date')
        # end_date = request.GET.get('end_date')
        """
        Menangani HTTP GET: 
        1. Jika ada param AJAX/Query Parameter khusus untuk list.
        2. Jika ada employee_id (Detail Info).
        3. Jika biasa, return render HTML view + Data Referensi.
        """
        # Kondisi A: Request Detail Karyawan (Jika ID dikirim lewat URL)
        if employee_id is not None:
            # Contoh query analitis menggunakan DatabaseConnector
            query = "SELECT * FROM employees_tb WHERE fn_userid = %s"
            data_detail = DatabaseConnector.execute_raw_sql(query, params=[employee_id], fetch_one=True)
            return Response(data_detail if data_detail else {}, status=status.HTTP_200_OK)

        # Kondisi B: Request Data List (Pengganti request()->ajax())
        # Di Django, kita bisa cek header ATAU query param khusus misal ?format=json
        if level:
            query_list = "SELECT * FROM employees_tb WHERE fn_userlevel = %s"
            raw_list = DatabaseConnector.execute_raw_sql(query_list, params=[level])
            
            # Pengolahan ala Data Analyst jika data list butuh dibersihkan/manipulasi cepat
            if raw_list:
                df = pd.DataFrame(raw_list)
                # Contoh: Mengganti nilai NaN atau Null dengan string kosong / '-' secara vektor
                df = df.replace({np.nan: '-'})
                return Response(df.to_dict(orient='records'), status=status.HTTP_200_OK)
            return Response([], status=status.HTTP_200_OK)

        # Kondisi C: Render View HTML Biasa (Default)
        query_ref = "SELECT fc_codecus, fv_namecus FROM customer_tb WHERE fc_hold = 'F'"
        referensi_data = DatabaseConnector.execute_raw_sql(query_ref)
        df_emp = pd.DataFrame(referensi_data)
        
        return JsonResponse({
            "referensi": df_emp.replace({np.nan: None}).to_dict(orient='records'),
            "level": level
        }, safe=False)


    def post(self, request, level, employee_id=None):
        """
        Menangani HTTP POST:
        Digunakan untuk setDetailInfo (Simpan/Update data karyawan)
        """
        data_input = request.data # Mengambil request->all() di Django
        
        # Logika simpan data menggunakan raw SQL via Connector Anda
        query_upsert = """
            INSERT INTO employees_tb (fn_userid, fv_nameemployees) 
            VALUES (%s, %s) 
            ON DUPLICATE KEY UPDATE fv_nameemployees = VALUES(fv_nameemployees)
        """
        # Eksekusi raw SQL aman
        DatabaseConnector.execute_raw_sql(
            query_upsert, 
            params=[data_input.get('userid'), data_input.get('name')],
            return_dict=False
        )
        
        return Response({"message": "Data berhasil disimpan"}, status=status.HTTP_200_OK)


class EmployeeReactivationController(APIView):
    """Controller terpisah untuk Reactivation agar sejalan dengan prinsip routing Django"""
    
    def post(self, request, level, employee_id):
        data_input = request.data
        
        query_reactivate = "UPDATE employees_tb SET fc_hold = 'F' WHERE fn_userid = %s"
        DatabaseConnector.execute_raw_sql(query_reactivate, params=[employee_id], return_dict=False)
        
        return Response({"message": "Karyawan berhasil diaktifkan kembali"}, status=status.HTTP_200_OK)

class EmployeeMasterView(DataLayerMixin,APIView):
    """
    Menangani halaman utama Master Employee, Create, dan Update.
    Mendukung pengolahan struktural via Pandas DataFrame.
    """
    def post(self, request):
            auth_header = request.headers.get('Authorization')
            if not auth_header:
                # 🛠️ PERBAIKAN 1: Gunakan DRF Response dan bungkus status code di dalam fungsi status=...
                return Response({"message": "Akses ditolak, token tidak ditemukan!"}, status=status.HTTP_401_UNAUTHORIZED)
                
            token = auth_header.split(" ")[1].strip()
            try:
                decoded_payload = jwt.decode(token, settings.SECRET_KEY, algorithms=["HS256"])
            except InvalidSignatureError:
                # 🛠️ PERBAIKAN 2: Ubah ke DRF Response agar konsisten
                return Response({'error': 'Token tidak valid / Signature failed'}, status=status.HTTP_401_UNAUTHORIZED)
            except ExpiredSignatureError:
                # 🛠️ PERBAIKAN 3: Ubah ke DRF Response agar konsisten
                return Response({'error': 'Token sudah kedaluwarsa'}, status=status.HTTP_401_UNAUTHORIZED)
                
            # Ambil 'type' dari body request JSON
            type_param = request.data.get('type')        
            try:
                # Ambil flag dry_run (default False jika tidak dikirim)
                dry_run = bool(request.data.get('dry_run', False))
                if type_param == 'upsert_employee':
                    # Eksekusi fungsi upsert dengan parameter dry_run
                    success, msg, metadata = self.upsert_employee(request, decoded_payload, dry_run=dry_run)
                    res_status = status.HTTP_200_OK if success else status.HTTP_400_BAD_REQUEST
                    return Response({
                        'status': 'success' if success else 'error',
                        'message': msg,
                        'data': metadata
                    }, status=res_status)
                elif type_param == 'bulk_update_employee_hold':
                    success, msg, metadata = self.bulk_update_employee_hold(request, decoded_payload)
                    res_status = status.HTTP_200_OK if success else status.HTTP_400_BAD_REQUEST
                    return Response({
                        'status': 'success' if success else 'error',
                        'message': msg,
                        'data': metadata
                    }, status=res_status)
                # 🛠️ PERBAIKAN 4: Ubah ke DRF Response agar konsisten
                return Response({
                    'success': False, 
                    'message': f"Method type '{type_param}' tidak valid atau tidak ditemukan."
                }, status=status.HTTP_400_BAD_REQUEST)
            except Exception as e:
                return _catch_error_details(e)
            
    def get(self, request):
        """
        Mengambil detail absensi (Pengganti getAttendanceDetail)
        """
        try:
            type_param = request.GET.get('type', 'master')
            print(type_param)
            if type_param == 'master':
                return self.get_master_list(request)
            elif type_param == 'master_listall':
                return self.get_list_all_employees(request)
            elif type_param == 'emp_detail':
                return self.get_detail_employees(request)
            else:
                return JsonResponse({'error': f"Type '{type_param}' tidak ditemukan"}, status=400)
        except Exception as e:
            return _catch_error_details(e)


    def get_master(self, request, level):
        # 1. Ambil data referensi dasar (PTKP)
        query_ptkp = "SELECT fn_ptkpid, fv_nama FROM pajak_ptkp"
        ptkp_list = DatabaseConnector.execute_raw_sql(query_ptkp)
        
        context = {
            'level': level,
            'ptkp': ptkp_list,
            'title': f'Master {level.capitalize()}'
        }

        # 2. Kondisi Khusus jika level == 'manager'
        if level == 'manager':
            context['title'] = 'Master Manager'
            
            # Ambil data bank
            query_bank = "SELECT fn_idbank, fv_bank FROM t_bank WHERE fc_status = 'T'"
            context['bank'] = DatabaseConnector.execute_raw_sql(query_bank)
            
            # Ambil data list supervisor menggunakan query join
            query_spv = """
                SELECT et.fn_userid as idSpv, et.fv_nameemployees as namaSpv
                FROM employees_tb as et
                INNER JOIN user_tb as ut ON et.fn_userid = ut.fn_userid
                WHERE et.fn_iddepart IN (8,20) 
                  AND ut.fn_userlevel IN (2,12)
                  AND et.fc_hold = 'F'
                ORDER BY et.fv_nameemployees ASC
            """
            raw_spv = DatabaseConnector.execute_raw_sql(query_spv)
            
            # Pendekatan Data Analyst: Pastikan tidak ada data kosong/null di output view
            if raw_spv:
                df_spv = pd.DataFrame(raw_spv)
                df_spv['namaSpv'] = df_spv['namaSpv'].fillna('-').str.upper() # Standardisasi nama kapital
                context['listSupervisor'] = df_spv.to_dict(orient='records')
            else:
                context['listSupervisor'] = []

            return render(request, 'desktop/employee/master_manager.html', context)

        # 3. Default Render untuk level lainnya (supervisor, operator, bazaar)
        if level == 'operator':
            context['title'] = 'Master SPG/B'
            
        return render(request, 'desktop/employee/master.html', context)
    
    def get_master_list(self,request):
        """
        Fungsi Analitis untuk mengambil list master karyawan berdasarkan level.
        Menggunakan DatabaseConnector untuk query cepat dan Pandas untuk rekayasa data.
        """
        level = request.GET.get('level')
        # 1. Penentuan Query dan Parameter berdasarkan Level Karyawan
        if level == 'manager':
            query = """
                SELECT
                    e.fc_codeemployees AS codeemployees,
                    e.fv_nameemployees AS nameemployees,
                    e.fn_idemployees AS idemployees,
                    e.fc_hold AS hold,
                    e.fv_username AS username,
                    DATE_FORMAT(e.fd_datein, '%Y-%m-%d') AS datein,
                    DATE_FORMAT(e.fd_dateout, '%Y-%m-%d') AS dateout
                FROM employees_manager_tb e
                ORDER BY e.fv_nameemployees ASC
            """
            params = []
        else:
            # Pemetaan tuple userlevel (mirip dengan switch-case di Laravel)
            level_map = {
                'operator': (1, 13, 8),
                'bazaar': (8,),       # Tuple single-element harus diakhiri koma
                'supervisor': (2, 12)
            }
            
            user_levels = level_map.get(level)
            if not user_levels:
                return JsonResponse({"draw": 1, "recordsTotal": 0, "recordsFiltered": 0, "data": []})

            # Ambil string dinamis untuk placeholder IN (%s, %s, ...)
            placeholders = ', '.join(['%s'] * len(user_levels))
            params = list(user_levels)
            ## 
            query = f"""
                SELECT
                    e.fc_codeemployees AS codeemployees,
                    e.fv_nameemployees AS nameemployees,
                    e.fn_idemployees AS idemployees,
                    e.fc_hold AS hold,
                    u.fv_username AS username,
                    DATE_FORMAT(e.fd_datein, '%%Y-%%m-%%d') AS datein,
                    DATE_FORMAT(e.fd_dateout, '%%Y-%%m-%%d') AS dateout
                FROM employees_tb e
                INNER JOIN (
                    SELECT fn_userid, fv_username 
                    FROM user_tb 
                    WHERE fn_userlevel IN ({placeholders})
                ) u ON e.fn_userid = u.fn_userid
                ORDER BY e.fv_nameemployees ASC
            """

        # 2. Eksekusi Query ke Database
        raw_data = DatabaseConnector.execute_raw_sql(query, params=params,using='payroll')
        ## Harus diganti ke 237 ##################################################################
        if not raw_data:
            return JsonResponse({"draw": 1, "recordsTotal": 0, "recordsFiltered": 0, "data": []})

        # 3. PENGOLAHAN DATA DENGAN PANDAS & NUMPY (Data Analyst Style)
        df = pd.DataFrame(raw_data)

        # Imputasi / Pembersihan Nilai Kosong (NaN/None)
        df['dateout'] = df['dateout'].fillna('-')
        df['datein'] = df['datein'].fillna('-')
        df['username'] = df['username'].fillna('-')
        
        # Rekayasa Fitur Status (Misal: Mengubah 'F' jadi 'Aktif' dan 'T' jadi 'Hold')
        df['status_label'] = np.where(df['hold'] == 'F', 'Aktif', 'Non-Aktif')

        # Replikasi `addIndexColumn()` milik DataTables Laravel (Index dimulai dari 1)
        df.insert(0, 'DT_RowIndex', np.arange(1, len(df) + 1))

        # 4. OUTPUT FORMAT DATATABLES SINKRONISASI
        # Mengembalikan struktur JSON sesuai yang diharapkan komponen DataTables frontend Anda
        records_count = len(df)
        result = {
            "draw": int(request.GET.get('draw', 1)),
            "recordsTotal": records_count,
            "recordsFiltered": records_count,
            "data": df.replace({np.nan: None}).to_dict(orient='records')
        }

        return JsonResponse(result, safe=False)
    def get_list_all_employees(self,request):
        """
        Fungsi analitis untuk menarik semua data karyawan dengan modifikasi layout 
        HTML secara masal menggunakan Pandas & Numpy Vectorization.
        """
        level = request.GET.get('level', '')
        
        # 1. STRUKTURISASI QUERY BERDASARKAN LEVEL (Data Sourcing)
        if level == 'manager':
            query = """
                SELECT
                    et.fv_nameemployees AS nama,
                    et.fc_codeemployees AS kode,
                    et.fv_nik AS nik,
                    et.fc_hold AS status,
                    DATE_FORMAT(et.fd_datein, '%%Y-%%m-%%d') AS tgl_bekerja,
                    DATE_FORMAT(et.fd_dateout, '%%Y-%%m-%%d') AS tgl_keluar,
                    et.fv_username AS username,
                    et.fn_idemployees AS id,
                    et.fv_nama_rekening AS namaPayroll,
                    '-' AS kode_stay,
                    '-' AS toko_stay
                FROM employees_manager_tb as et
                ORDER BY et.fv_nameemployees ASC
            """
            params = []
            dbUse='payroll'
        else:
            # Tentukan parameter level_id (tuple sql safely)
            level_ids = (1, 8, 13) if level == 'spg' else (2, 12)
            placeholders = ', '.join(['%s'] * len(level_ids))
            params = list(level_ids)

            # Kondisi Join khusus untuk Operator (SPG)
            join_operator = ""
            select_operator = "'-' AS kode_stay, '-' AS toko_stay,"
            if level == 'spg':
                join_operator = """
                    INNER JOIN t_aksesusercust as ta ON ut.fn_userid = ta.fn_userid
                    INNER JOIN customer_tb as ct ON ta.fn_cusid = ct.fn_cusid
                """
                select_operator = "ct.fc_codecus AS kode_stay, ct.fv_namecus AS toko_stay,"
            ## 
            query = f"""
                SELECT
                    et.fv_nameemployees AS nama,
                    et.fc_codeemployees AS kode,
                    et.fv_nik AS nik,
                    et.fc_hold AS status,
                    DATE_FORMAT(et.fd_datein, '%%Y-%%m-%%d') AS tgl_bekerja,
                    DATE_FORMAT(et.fd_dateout, '%%Y-%%m-%%d') AS tgl_keluar,
                    ut.fv_username AS username,
                    et.fn_idemployees AS id,
                    ut.fn_userid,
                    ut.fc_hold AS status_u,
                    {select_operator}
                    '-' AS namaPayroll
                FROM employees_tb as et
                INNER JOIN user_tb as ut ON et.fn_userid = ut.fn_userid
                {join_operator}
                WHERE et.fn_iddepart IN (8, 20) 
                AND ut.fn_userlevel IN ({placeholders})
                GROUP BY et.fn_idemployees
                ORDER BY et.fv_nameemployees ASC
            """

        # 2. EKSEKUSI DATABASE CONNECTOR
        raw_data = DatabaseConnector.execute_raw_sql(query, params=params,using='payroll')
        ## Harus diganti ke 237 ##################################################################
        if not raw_data:
            return JsonResponse({"data": []})

        # 3. PENGOLAHAN MASAL DENGAN PANDAS & NUMPY (Data Manipulation & Analytics)
        df = pd.DataFrame(raw_data)
        
        if level != 'manager':
            print('good')
        # Imputasi data kosong (Handling missing values)
        df['tgl_bekerja'] = df['tgl_bekerja'].fillna('-')
        df['tgl_keluar'] = df['tgl_keluar'].fillna('-')
        df['toko_stay'] = df['toko_stay'].fillna('-')
        df['kode_stay'] = df['kode_stay'].fillna('-')
        df['username'] = df['username'].fillna('-').str.upper() # Manipulasi string kapital masal
        
        # Standarisasi Penulisan Nama (Title Case)
        df['nama'] = df['nama'].fillna('').str.title()

        # Membungkus teks ke dalam tag HTML menggunakan ekspresi vektor Pandas
        # df['nama'] = '<small>' + df['nama'] + '</small>'
        # df['kode'] = '<small>' + df['kode'].astype(str) + '</small>'
        # df['toko_stay'] = '<small>' + df['toko_stay'] + '</small>'

        # Rekayasa Fitur Status (Hold T = non-aktif, else = aktif)
        df['status'] = np.where(df['status'] == 'T', 'non-aktif', 'aktif')
        df['status_u'] = np.where(df['status_u'] == 'T', 'non-aktif', 'aktif')

        # Pembuatan Komponen Tombol Aksi HTML Secara Vektor Berbasis ID Karyawan
        # df['opsi'] = (
        #     '<div class="btn-group btn-group-sm">'
        #     '<button type="button" class="btn btn-sm bg-primary" onclick="openDetail(' + df['id'].astype(str) + ')">Update</button>'
        #     '<button type="button" class="btn btn-sm bg-purple" onclick="riwayatKlaim(' + df['id'].astype(str) + ', this, \'klaim\')">Klaim</button>'
        #     '<button type="button" class="btn btn-sm bg-olive" onclick="riwayatKlaim(' + df['id'].astype(str) + ', this, \'seragam\')">Seragam</button>'
        #     '</div>'
        # )

        # Replikasi $key + 1 (Membuat Nomor Index Urut Mulai dari 1)
        df.insert(0, 'DT_Row_Index', np.arange(1, len(df) + 1))
        records_count = len(df)
        # 4. JSON OUTPUT GENERATION
        # Gunakan orient='records' agar langsung membentuk format array of dictionary sesuai DataTable
        return JsonResponse({
            "recordsTotal": records_count,
            "data": df.to_dict(orient='records'),
                             }, safe=False)
    
    def get_detail_employees(self,request):
        userid = request.GET.get('userid')
        df_uemp=self.df_toko_handle_(
            colom=("u.*, e.*"),
            queri=False,
            use_default_filters=False,
            u__fn_userid= userid
        )
        df_cuaks=self.df_toko_handle_(
            colom=("c.fn_cusid,fc_codecus,fv_namecus"),
            queri=False,
            use_default_filters=False,
            u__fn_userid= userid,
            t__fc_status= "T",
        )
        df_cus_stay=self.df_cus_stay_(colom=("fn_userid,fn_cusid,fc_up_one"),
            fn_userid= userid)
        df_cus_stay=df_cus_stay.merge(df_cuaks[['fv_namecus','fn_cusid']],on='fn_cusid',how='left')
        df_empspg=self.df_empspg_(
                    queri=False,
                    fn_userid= userid
                )
                
        df_uemp=df_uemp.merge(df_empspg,on='fn_userid',how='left')
        return JsonResponse({
                    "data_emp": df_uemp.to_dict(orient='records'),
                    # "data_empspg": df_empspg.to_dict(orient='records'),
                    "data_cus_akses": df_cuaks.to_dict(orient='records'),
                    "data_cus_akses2": df_cus_stay.to_dict(orient='records'),
                                     }, safe=False)
    
    def check_username_exists(self, username: str, exclude_user_id: int = None) -> bool:
        """Cek ketersediaan username di DB."""
        if not username:
            return False
        query = "SELECT COUNT(*) AS total FROM user_tb WHERE LOWER(fv_username) = LOWER(%s)"
        params = [username.strip()]
        if exclude_user_id:
            query += " AND fn_userid <> %s"
            params.append(exclude_user_id)
        
        res = DatabaseConnector.execute_raw_sql(query, params=params, using=dbSave, fetch_one=True)
        return (res.get('total', 0) if res else 0) > 0

    def _hash_md5(self, text: str) -> str:
        return hashlib.md5(text.encode('utf-8')).hexdigest()

    def _generate_employee_code(self) -> str:
        # Ambil 4 digit terakhir sebagai INT, cari nilai tertinggi yang ber-prefix 'EMS-'
        query_max = """
            SELECT MAX(CAST(SUBSTRING_INDEX(fc_codeemployees, '-', -1) AS UNSIGNED)) AS max_seq 
            FROM gis_db.employees_tb 
            WHERE fc_codeemployees LIKE 'EMS-%%'
            -- Memastikan bagian setelah '-' adalah angka murni
            AND SUBSTRING_INDEX(fc_codeemployees, '-', -1) REGEXP '^[0-9]+$'
        """
        res = DatabaseConnector.execute_raw_sql(query_max, using=dbSave, fetch_one=True)
        
        # Ambil nilai max, jika belum ada data/None maka mulai dari 0
        max_seq = res.get('max_seq') if res and res.get('max_seq') is not None else 0
        next_seq = max_seq + 1
        
        return f"EMS-{next_seq:04d}"
    
    def _get_next_user_id(self) -> int:
        """
        Mengambil calon fn_userid (AUTO_INCREMENT) selanjutnya dari user_tb tanpa insert.
        """
        query = """
            SELECT AUTO_INCREMENT AS next_id 
            FROM information_schema.tables 
            WHERE table_name = 'user_tb' 
            AND table_schema = DATABASE()
        """
        res = DatabaseConnector.execute_raw_sql(query, using=dbSave, fetch_one=True)
        if res and res.get('next_id'):
            return int(res.get('next_id'))
        
        # Fallback jika information_schema tidak diizinkan: Ambil MAX(fn_userid) + 1
        query_max = "SELECT COALESCE(MAX(fn_userid), 0) + 1 AS next_id FROM user_tb"
        res_max = DatabaseConnector.execute_raw_sql(query_max, using=dbSave, fetch_one=True)
        return int(res_max.get('next_id', 1))
    
    def upsert_employee(self, request, decoded_payload, dry_run: bool = False):
        """
        Single Method untuk Create/Update Data Employee Multi-Table (dbPRS & dbPRoll).
        Mendukung mode dry_run=True untuk inspeksi query dan simulasi isi field tanpa commit DB.
        """
        try:
            now = datetime.now()
            data = request.data
            print(data)
            actor_id = decoded_payload.get('userid') or request.data.get('userid') or 1
            
            # 1. Parsing Input Parameter
            # FIX 1: target_user_id harus membaca 'fn_userid' atau 'userid' target, BUKAN 'cus_stay'
            
            user_data = data.get('user', {})
            target_user_id = user_data.get('fn_userid') or data.get('userid_target')
            username = user_data.get('fv_username', '') or data.get('fv_username', '')
            password = user_data.get('fv_password_web') or data.get('fv_password_web')
            userlevel = user_data.get('fn_userlevel') or data.get('fn_userlevel')
            cus_stay = data.get('cus_stay')
            
            # Customer IDs List
            customer_ids = data.get('customer_ids', [])
            if not customer_ids and data.get('fn_cusid'):
                fn_cus = data.get('fn_cusid')
                customer_ids = fn_cus if isinstance(fn_cus, list) else [fn_cus]

            # Penentuan fc_up_one (Jika customer_ids > 1 maka 'T', jika 1 atau kosong maka 'F')
            fc_up_one_flag = 'T' if len(customer_ids) > 1 else 'F'

            # Penentuan 1 cus_stay_id
            target_cus_stay = cus_stay
            if not target_cus_stay and customer_ids:
                target_cus_stay = customer_ids[0]

            if isinstance(target_cus_stay, list) and target_cus_stay:
                target_cus_stay = target_cus_stay[0]

            is_update = bool(target_user_id)
            dry_run_summary = {
                "is_dry_run": True,
                "action": "UPDATE" if is_update else "INSERT",
                "actor_id": actor_id,
                "target_user_id": target_user_id,
                "queries_to_execute": [],
                "table_payloads": {}
            }

            # =================================================================
            # MODE: UPDATE
            # =================================================================
            if is_update:
                if username and self.check_username_exists(username, exclude_user_id=target_user_id):
                    return False, f"Username '{username}' sudah digunakan user lain.", {}

                # A. Prepare user_tb Update
                update_fields = []
                update_params = []
                user_tb_simulated = {}

                if username:
                    update_fields.append("fv_username = %s")
                    update_params.append(username)
                    user_tb_simulated["fv_username"] = username
                if password:
                    update_fields.append("fv_password_web = %s")
                    update_params.append(self._hash_md5(password))
                    user_tb_simulated["fv_password_web"] = "[HASHED_MD5]"
                if userlevel:
                    update_fields.append("fn_userlevel = %s")
                    update_params.append(userlevel)
                    user_tb_simulated["fn_userlevel"] = userlevel

                if update_fields:
                    query_update_user = f"UPDATE user_tb SET {', '.join(update_fields)} WHERE fn_userid = %s"
                    update_params.append(target_user_id)
                    
                    dry_run_summary["queries_to_execute"].append({
                        "db": "dbPRS",
                        "query": query_update_user,
                        "params": update_params
                    })
                    dry_run_summary["table_payloads"]["user_tb"] = user_tb_simulated

                    if not dry_run:
                        DatabaseConnector.execute_raw_sql(query_update_user, params=update_params, using=dbPRS)
                
                # B. Sync t_aksesusercust
                if customer_ids is not None:
                    # 1. Ambil data fn_cusid yang saat ini sudah ada di database untuk user ini
                    query_existing = "SELECT fn_cusid FROM t_aksesusercust WHERE fn_userid = %s"
                    existing_records = DatabaseConnector.execute_raw_sql(
                        query_existing, params=[target_user_id], using=dbPRS
                    )
                    existing_cusids = set()
                    if existing_records:
                        for row in existing_records:
                            if isinstance(row, (tuple, list)):
                                existing_cusids.add(row[0])
                            elif isinstance(row, dict):
                                existing_cusids.add(row.get('fn_cusid'))
                            else:  # Jika return langsung list integer [171]
                                existing_cusids.add(row)
                    target_cusids = set(customer_ids)
                    # 2. Tentukan mana yang perlu di-INSERT, UPDATE ke 'T', dan UPDATE ke 'F'
                    to_insert = target_cusids - existing_cusids
                    to_activate = target_cusids & existing_cusids
                    to_deactivate = existing_cusids - target_cusids
                    # Query Templates
                    query_ins_akses = """
                        INSERT INTO t_aksesusercust (fn_userid, fn_cusid, fc_status, fn_userinput, fd_dateinput)
                        VALUES (%s, %s, 'T', %s, %s)
                    """
                    query_update_status = """
                        UPDATE t_aksesusercust 
                        SET fc_status = %s, fn_userinput = %s, fd_dateinput = %s
                        WHERE fn_userid = %s AND fn_cusid = %s
                    """
                    # --- POPULATE DRY RUN & EXECUTION ---
                    # A. PROSES DEAKTIVASI (Status = 'F')
                    for cid in to_deactivate:
                        params_deact = ['F', actor_id, now, target_user_id, cid]
                        dry_run_summary["queries_to_execute"].append({
                            "db": "dbPRS", "query": query_update_status, "params": params_deact
                        })
                        if not dry_run:
                            DatabaseConnector.execute_raw_sql(query_update_status, params=params_deact, using=dbPRS)

                    # B. PROSES RE-AKTIVASI (Status = 'T')
                    for cid in to_activate:
                        params_act = ['T', actor_id, now, target_user_id, cid]
                        dry_run_summary["queries_to_execute"].append({
                            "db": "dbPRS", "query": query_update_status, "params": params_act
                        })
                        if not dry_run:
                            DatabaseConnector.execute_raw_sql(query_update_status, params=params_act, using=dbPRS)

                    # C. PROSES INSERT DATA BARU
                    for cid in to_insert:
                        params_ins = [target_user_id, cid, actor_id, now]
                        dry_run_summary["queries_to_execute"].append({
                            "db": "dbPRS", "query": query_ins_akses, "params": params_ins
                        })
                        if not dry_run:
                            DatabaseConnector.execute_raw_sql(query_ins_akses, params=params_ins, using=dbPRS)

                    # Direct Payloads Summary (Aktivitas Aktif Saat Ini)
                    dry_run_summary["table_payloads"]["t_aksesusercust"] = [
                        {
                            "fn_userid": target_user_id,
                            "fn_cusid": cid,
                            "fc_status": "T",
                            "fn_userinput": actor_id,
                            "fd_dateinput": now.strftime("%Y-%m-%d %H:%M:%S")
                        } for cid in target_cusids
                    ]

                # C. Sync m_cus_stay pada DB_PROLL
                if target_cus_stay:
                    del_stay = "DELETE FROM m_cus_stay WHERE fn_userid = %s"
                    query_ins_stay = """
                        INSERT INTO m_cus_stay (fn_userid, fn_cusid, fc_up_one, fc_hold, fn_userid_in)
                        VALUES (%s, %s, %s, 'F', %s)
                    """
                    
                    dry_run_summary["queries_to_execute"].append({"db": "dbPRoll", "query": del_stay, "params": [target_user_id]})
                    dry_run_summary["table_payloads"]["m_cus_stay"] = [{
                        "fn_userid": target_user_id,
                        "fn_cusid": target_cus_stay,
                        "fc_up_one": fc_up_one_flag,
                        "fc_hold": "F",
                        "fn_userid_in": actor_id
                    }]

                    dry_run_summary["queries_to_execute"].append({
                        "db": "dbPRoll",
                        "query": query_ins_stay,
                        "params": [target_user_id, target_cus_stay, fc_up_one_flag, actor_id]
                    })

                    if not dry_run:
                        DatabaseConnector.execute_raw_sql(del_stay, params=[target_user_id], using=dbPRoll)
                        DatabaseConnector.execute_raw_sql(
                            query_ins_stay, 
                            params=[target_user_id, target_cus_stay, fc_up_one_flag, actor_id], 
                            using=dbPRoll
                        )

                # FIX 2: Menambahkan RETURN statement di akhir blok MODE UPDATE
                msg = f"[DRY-RUN] Simulasi update employee (ID: {target_user_id}) berhasil." if dry_run else f"Data employee (ID: {target_user_id}) berhasil diperbarui."
                return True, msg, dry_run_summary if dry_run else {"fn_userid": target_user_id}

            # =================================================================
            # MODE: INSERT (CREATE NEW)
            # =================================================================
            else:
                if not username:
                    return False, "Username wajib diisi untuk registrasi baru.", {}

                if self.check_username_exists(username):
                    return False, f"Username '{username}' sudah digunakan. Gunakan username lain.", {}

                emp_code = self._generate_employee_code()
                
                if dry_run:
                    new_user_id = self._get_next_user_id() 
                
                # 1. user_tb
                query_ins_user = """
                    INSERT INTO user_tb (fv_username, fv_password_web, fc_hold, fn_userlevel, fn_userinput)
                    VALUES (%s, %s, 'F', %s, %s)
                """
                params_ins_user = [username, self._hash_md5(password or ''), userlevel or 1, actor_id]

                dry_run_summary["queries_to_execute"].append({
                    "db": "dbPRS",
                    "query": query_ins_user,
                    "params": params_ins_user
                })
                dry_run_summary["table_payloads"]["user_tb"] = {
                    "fn_userid": new_user_id if dry_run else "(Auto Increment DB)",
                    "fv_username": username,
                    "fv_password_web": self._hash_md5(password or '') if password else None,
                    "fc_hold": "F",
                    "fn_userlevel": userlevel or 1,
                    "fn_userinput": actor_id
                }

                if not dry_run:
                    new_user_id = DatabaseConnector.execute_raw_sql(
                        query_ins_user, params=params_ins_user, using=dbPRS, return_last_id=True
                    )
                    if not new_user_id:
                        raise Exception("Gagal mendapatkan ID baru dari user_tb.")

                # 2. employees_tb
                query_ins_emp = """
                    INSERT INTO employees_tb (fc_codeemployees, fn_userid, fn_iddepart, fn_userinput, fd_dateinput, fc_hold)
                    VALUES (%s, %s, 8, %s, %s,"F")
                """
                dry_run_summary["queries_to_execute"].append({
                    "db": "dbPRS",
                    "query": query_ins_emp,
                    "params": [emp_code, new_user_id, actor_id, now]
                })
                dry_run_summary["table_payloads"]["employees_tb"] = {
                    "fc_codeemployees": emp_code,
                    "fn_userid": new_user_id,
                    "fn_iddepart": 8,
                    "fn_userinput": actor_id,
                    "fd_dateinput": now.strftime("%Y-%m-%d %H:%M:%S")
                }

                if not dry_run:
                    DatabaseConnector.execute_raw_sql(query_ins_emp, params=[emp_code, new_user_id, actor_id, now], using=dbPRS)

                # 3. t_aksesusercust
                query_ins_akses = """
                    INSERT INTO t_aksesusercust (fn_userid, fn_cusid, fc_status, fn_userinput, fd_dateinput)
                    VALUES (%s, %s, 'T', %s, %s)
                """
                dry_run_summary["table_payloads"]["t_aksesusercust"] = [
                    {
                        "fn_userid": new_user_id,
                        "fn_cusid": cid,
                        "fc_status": "T",
                        "fn_userinput": actor_id,
                        "fd_dateinput": now.strftime("%Y-%m-%d %H:%M:%S")
                    } for cid in customer_ids
                ]

                for cid in customer_ids:
                    dry_run_summary["queries_to_execute"].append({
                        "db": "dbPRS",
                        "query": query_ins_akses,
                        "params": [new_user_id, cid, actor_id, now]
                    })
                    if not dry_run:
                        DatabaseConnector.execute_raw_sql(query_ins_akses, params=[new_user_id, cid, actor_id, now], using=dbPRS)

                # 4. m_cus_stay
                if target_cus_stay:
                    query_ins_stay = """
                        INSERT INTO m_cus_stay (fn_userid, fn_cusid, fc_up_one, fc_hold, fn_userid_in)
                        VALUES (%s, %s, %s, 'F', %s)
                    """
                    
                    dry_run_summary["table_payloads"]["m_cus_stay"] = [{
                        "fn_userid": new_user_id,
                        "fn_cusid": target_cus_stay,
                        "fc_up_one": fc_up_one_flag,
                        "fc_hold": "F",
                        "fn_userid_in": actor_id
                    }]

                    dry_run_summary["queries_to_execute"].append({
                        "db": "dbPRoll",
                        "query": query_ins_stay,
                        "params": [new_user_id, target_cus_stay, fc_up_one_flag, actor_id]
                    })

                    if not dry_run:
                        DatabaseConnector.execute_raw_sql(
                            query_ins_stay, 
                            params=[new_user_id, target_cus_stay, fc_up_one_flag, actor_id], 
                            using=dbPRoll
                        )

                msg = f"[DRY-RUN] Simulasi insert employee baru ({emp_code}) berhasil." if dry_run else f"Employee baru ({emp_code}) berhasil disimpan."
                return True, msg, dry_run_summary if dry_run else {"fn_userid": new_user_id, "code": emp_code}

        except Exception as e:
            return False, f"Terjadi kesalahan pada server: {str(e)}", {}
    from django.db import transaction
    from django.db.models import Q

    def upsert_employee(self, request, decoded_payload, dry_run: bool = False):
        """
        Single Method untuk Create/Update Data Employee Multi-Table (dbPRS & dbPRoll)
        menggunakan Django ORM secara efisien.
        """
        try:
            data = request.data
            actor_id = decoded_payload.get('userid') or data.get('userid') or 1
            client_ip = _get_client_ip(request)

            user_data = data.get('user', {})
            target_user_id = parse_int_nullable(user_data.get('fn_userid') or data.get('userid_target'))
            username = user_data.get('fv_username', '').strip() or data.get('fv_username', '').strip()
            password = user_data.get('fv_password_web') or data.get('fv_password_web')
            userlevel = parse_int_nullable(user_data.get('fn_userlevel') or data.get('fn_userlevel'))
            
            # Customer IDs Processing
            cus_stay = data.get('cus_stay')
            customer_ids = data.get('customer_ids', [])
            if not customer_ids and data.get('fn_cusid'):
                fn_cus = data.get('fn_cusid')
                customer_ids = fn_cus if isinstance(fn_cus, list) else [fn_cus]

            target_cusids = set(customer_ids)
            fc_up_one_flag = 'T' if len(target_cusids) > 1 else 'F'

            target_cus_stay = cus_stay
            if not target_cus_stay and target_cusids:
                target_cus_stay = list(target_cusids)[0]
            if isinstance(target_cus_stay, list) and target_cus_stay:
                target_cus_stay = target_cus_stay[0]

            is_update = bool(target_user_id)
            current_time = timezone.now()
            
            dry_run_summary = {
                "is_dry_run": dry_run,
                "action": "UPDATE" if is_update else "INSERT",
                "actor_id": actor_id,
                "target_user_id": target_user_id,
                "table_payloads": {}
            }

            fd_datein_val = parse_date_nullable(user_data.get('fd_datein') or data.get('fd_datein'))
            fd_dateout_val = parse_date_nullable(user_data.get('fd_dateout') or data.get('fd_dateout'))
            fd_tgllahir_val = parse_date_nullable(user_data.get('fd_tgllahir') or data.get('fd_tgllahir'))
            fd_tglawalkontrak_val = parse_date_nullable(user_data.get('fd_tglawalkontrak') or data.get('fd_tglawalkontrak'))
            fd_tglakhirkontrak_val = parse_date_nullable(user_data.get('fd_tglakhirkontrak') or data.get('fd_tglakhirkontrak'))

            emp_payload = user_data if user_data else data

            with transaction.atomic(using=dbPRS), transaction.atomic(using=dbPRoll):

                # =================================================================
                # MODE 1: UPDATE EMPLOYEE
                # =================================================================
                if is_update:
                    if username and UserTb.objects.using(dbPRS).filter(fv_username=username).exclude(fn_userid=target_user_id).exists():
                        return False, f"Username '{username}' sudah digunakan user lain.", {}

                    # -------------------------------------------------------------
                    # A. Update UserTb
                    # -------------------------------------------------------------
                    user_obj = UserTb.objects.using(dbPRS).filter(fn_userid=target_user_id).first()
                    if not user_obj:
                        return False, f"User dengan ID {target_user_id} tidak ditemukan.", {}

                    user_changed = False
                    if password:
                        hashed_pass = self._hash_md5(password)
                        if user_obj.fv_password_web != hashed_pass:
                            user_obj.fv_password_web = hashed_pass
                            user_changed = True
                    if userlevel is not None and user_obj.fn_userlevel != userlevel:
                        user_obj.fn_userlevel = userlevel
                        user_changed = True
                    if user_data.get('fc_hold_u') and user_obj.fc_hold != user_data.get('fc_hold_u'):
                        user_obj.fc_hold = user_data.get('fc_hold_u')
                        user_changed = True

                    if user_changed and not dry_run:
                        user_obj.save(using=dbPRS)

                        HsUserTb.objects.using(dbPRS).create(
                            fn_userid=user_obj.fn_userid,
                            fv_username=user_obj.fv_username,
                            fd_expireddate=user_obj.fd_expireddate,
                            fc_hold=user_obj.fc_hold,
                            fc_changepassword=user_obj.fc_changepassword,
                            fv_desc=user_obj.fv_desc,
                            fv_password_web=user_obj.fv_password_web,
                            fv_email=user_obj.fv_email,
                            fn_counter=user_obj.fn_counter,
                            fn_userlevel=user_obj.fn_userlevel,
                            fn_whid=user_obj.fn_whid,
                            fn_userinput=user_obj.fn_userinput,
                            fd_dateinput=user_obj.fd_dateinput,
                            fv_ipinput=user_obj.fv_ipinput,
                            fn_userupdate=actor_id,
                            fd_dateupdate=current_time,
                            fv_ipupdate=client_ip,
                            fc_action="U",
                        )

                    # -------------------------------------------------------------
                    # B. Upsert EmployeesTb (Patch Only)
                    # -------------------------------------------------------------
                    emp_obj = EmployeesTb.objects.using(dbPRS).filter(fn_userid=target_user_id).first()
                    if not emp_obj:
                        emp_obj = EmployeesTb(fn_userid=target_user_id)

                    self._patch_employee_instance(emp_obj, emp_payload, fd_tgllahir_val, fd_dateout_val, fd_datein_val)
                    emp_obj.fn_userinput = actor_id

                    if not dry_run:
                        emp_obj.save(using=dbPRS)

                        HsEmployeesTb.objects.using(dbPRS).create(
                            fn_userid=emp_obj.fn_userid,
                            fn_idemployees=emp_obj.fn_idemployees,
                            fv_alias=emp_obj.fv_alias,
                            fc_codeemployees=emp_obj.fc_codeemployees,
                            fv_nameemployees=emp_obj.fv_nameemployees,
                            fv_npwp=emp_obj.fv_npwp,
                            fv_nip=emp_obj.fv_nip,
                            fv_nik=emp_obj.fv_nik,
                            fv_address=emp_obj.fv_address,
                            fd_datein=emp_obj.fd_datein,
                            fd_dateout=emp_obj.fd_dateout,
                            fc_staff=emp_obj.fc_staff,
                            fn_userinput=emp_obj.fn_userinput,
                            fd_dateinput=emp_obj.fd_dateinput,
                            fv_ipinput=emp_obj.fv_ipinput,
                            fv_rt=emp_obj.fv_rt,
                            fv_rw=emp_obj.fv_rw,
                            fv_nomorrekbca=emp_obj.fv_nomorrekbca,
                            fv_jeniskelamin=emp_obj.fv_jeniskelamin,
                            fd_tgllahir=emp_obj.fd_tgllahir,
                            fv_nomorKK=emp_obj.fv_nomorKK,
                            fn_kotalahir=emp_obj.fn_kotalahir,
                            fv_pendidikanakhir=emp_obj.fv_pendidikanakhir,
                            fv_email=emp_obj.fv_email,
                            fc_hold=emp_obj.fc_hold,
                            fc_action='U',
                            fn_userupdate=actor_id,
                            fd_dateupdate=current_time
                        )

                    # -------------------------------------------------------------
                    # C. Upsert EmployeesspgTb (Patch Only)
                    # -------------------------------------------------------------
                    spg_obj = EmployeesspgTb.objects.using(dbLoad).filter(fn_userid=target_user_id).first()
                    if not spg_obj:
                        spg_obj = EmployeesspgTb(fn_userid=target_user_id)

                    self._patch_spg_instance(spg_obj, emp_payload, target_cus_stay, fd_tglawalkontrak_val, fd_tglakhirkontrak_val)
                    spg_obj.fn_userid_up = actor_id

                    if not dry_run:
                        spg_obj.save(using=dbLoad)

                    # -------------------------------------------------------------
                    # D. Upsert Akses User Customer (t_aksesusercust)
                    # -------------------------------------------------------------
                    if target_cusids:
                        self._sync_user_customer_access(target_user_id, target_cusids, actor_id, dry_run)

                    # -------------------------------------------------------------
                    # E. Upsert m_cus_stay
                    # -------------------------------------------------------------
                    if target_cus_stay and not dry_run:
                        MCusStay.objects.using(dbPRoll).filter(fn_userid=target_user_id).delete()
                        MCusStay.objects.using(dbPRoll).create(
                            fn_userid=target_user_id,
                            fn_cusid=target_cus_stay,
                            fc_up_one=fc_up_one_flag,
                            fc_hold='F',
                            fn_userid_in=actor_id
                        )

                    dry_run_summary["table_payloads"] = {
                        "user_tb": {"fv_username": user_obj.fv_username, "fn_userlevel": user_obj.fn_userlevel},
                        "employees_tb": emp_obj.__dict__,
                        "employeesspg_tb": spg_obj.__dict__,
                        "t_aksesusercust": [{"fn_cusid": cid, "fc_status": "T"} for cid in target_cusids],
                        "m_cus_stay": {"fn_userid": target_user_id, "fn_cusid": target_cus_stay, "fc_up_one": fc_up_one_flag}
                    }

                    msg = f"[DRY-RUN] Simulasi update employee (ID: {target_user_id}) berhasil." if dry_run else f"Data employee (ID: {target_user_id}) berhasil diperbarui."
                    return True, msg, dry_run_summary if dry_run else {"fn_userid": target_user_id}

                # =================================================================
                # MODE 2: INSERT EMPLOYEE BARU (Lengkap dengan Customer Akses & Stay)
                # =================================================================
                else:
                    if not username:
                        return False, "Username wajib diisi untuk registrasi baru.", {}
                    if UserTb.objects.using(dbPRS).filter(fv_username=username).exists():
                        return False, f"Username '{username}' sudah digunakan.", {}
                    emp_code = self._generate_employee_code()
                    if dry_run:
                        new_user_id = self._get_next_user_id()
                    else:
                        # 1. Save UserTb
                        user_obj = UserTb.objects.using(dbPRS).create(
                            fv_username=username,
                            fv_password_web=self._hash_md5(password or ''),
                            fc_hold='F',
                            fn_userlevel=userlevel or 1,
                            fn_userinput=actor_id
                        )
                        new_user_id = user_obj.fn_userid
                        # 2. Save EmployeesTb
                        emp_obj = EmployeesTb(
                            fc_codeemployees=emp_code,
                            fn_userid=new_user_id,
                            fd_datein=fd_datein_val or current_time,
                            fd_dateinput=current_time,
                            fn_userinput=actor_id,
                        )
                        self._patch_employee_instance(emp_obj, emp_payload, fd_tgllahir_val, fd_dateout_val, fd_datein_val)
                        emp_obj.save(using=dbPRS)
                        # 3. Save EmployeesspgTb
                        spg_obj = EmployeesspgTb(fn_userid=new_user_id, fn_userid_up=actor_id)
                        self._patch_spg_instance(spg_obj, emp_payload, target_cus_stay, fd_tglawalkontrak_val, fd_tglakhirkontrak_val)
                        spg_obj.save(using=dbLoad)
                        # 4. Save TAksesusercust (Toko Access)
                        if target_cusids:
                            records_to_create = [
                                TAksesusercust(
                                    fn_userid=new_user_id,
                                    fn_cusid=cid,
                                    fc_status='T',
                                    fn_userinput=actor_id
                                ) for cid in target_cusids
                            ]
                            TAksesusercust.objects.using(dbPRS).bulk_create(records_to_create)
                        # 5. Save MCusStay (Toko Stay)
                        if target_cus_stay:
                            MCusStay.objects.using(dbPRoll).create(
                                fn_userid=new_user_id,
                                fn_cusid=target_cus_stay,
                                fc_up_one=fc_up_one_flag,
                                fc_hold='F',
                                fn_userid_in=actor_id
                            )
                    dry_run_summary["table_payloads"] = {
                        "user_tb": {"fv_username": username, "fn_userlevel": userlevel or 1},
                        "employees_tb": {"fc_codeemployees": emp_code, "fn_userid": new_user_id},
                        "t_aksesusercust": [{"fn_cusid": cid, "fc_status": "T"} for cid in target_cusids],
                        "m_cus_stay": {"fn_userid": new_user_id, "fn_cusid": target_cus_stay, "fc_up_one": fc_up_one_flag}
                    }
                    msg = f"[DRY-RUN] Simulasi insert employee baru berhasil." if dry_run else f"Data employee baru berhasil ditambahkan."
                    return True, msg, dry_run_summary if dry_run else {"fn_userid": new_user_id, "fc_codeemployees": emp_code}
        except Exception as e:
            raise e
    # Helper methods tambahan di dalam class:

    def _patch_employee_instance(self, emp_obj, payload, fd_tgllahir_val, fd_dateout_val, fd_datein_val):
        for key in ['fv_nameemployees', 'fv_nik', 'fv_address', 'fc_staff', 'fv_jeniskelamin', 
                    'fv_rt', 'fv_rw', 'fv_zipcode', 'fv_email', 'fv_pendidikanakhir', 
                    'fv_nomorKK', 'fv_npwp', 'fv_alamatdomisili', 'fv_nomorrekbca']:
            if key in payload:
                setattr(emp_obj, key, payload[key])
        if 'fc_hold_e' in payload:
            emp_obj.fc_hold = payload['fc_hold_e']
        if 'fn_kotalahir' in payload:
            emp_obj.fn_kotalahir = parse_int_nullable(payload['fn_kotalahir'])
        if fd_tgllahir_val is not None:
            emp_obj.fd_tgllahir = fd_date_to_date_only(fd_tgllahir_val)
        if fd_dateout_val is not None:
            emp_obj.fd_dateout = fd_dateout_val
        if fd_datein_val is not None:
            emp_obj.fd_datein = fd_datein_val

    def _patch_spg_instance(self, spg_obj, payload, target_cus_stay, fd_tglawalkontrak_val, fd_tglakhirkontrak_val):
        for key in ['fv_status_pekerja', 'fv_type_pekerja', 'fv_refkerja', 'fv_keterangan', 'fv_nama_rekening', 'fv_cabang_rekening']:
            if key in payload:
                setattr(spg_obj, key, payload[key])
        for key in ['fn_idagama', 'fn_mirrageid', 'fn_anak', 'fn_idbank', 'fn_ptkpid']:
            if key in payload:
                setattr(spg_obj, key, parse_int_nullable(payload[key]))
        if target_cus_stay is not None:
            spg_obj.fn_cusid = parse_int_nullable(target_cus_stay) or 0
        if fd_tglawalkontrak_val is not None:
            spg_obj.fd_tglawalkontrak = fd_tglawalkontrak_val
        if fd_tglakhirkontrak_val is not None:
            spg_obj.fd_tglakhirkontrak = fd_tglakhirkontrak_val

    def _sync_user_customer_access(self, target_user_id, target_cusids, actor_id, dry_run):
        existing_qs = TAksesusercust.objects.using(dbPRS).filter(fn_userid=target_user_id)
        existing_map = {obj.fn_cusid: obj for obj in existing_qs}
        existing_cusids = set(existing_map.keys())

        to_insert_ids = target_cusids - existing_cusids
        to_activate_ids = target_cusids & existing_cusids
        to_deactivate_ids = existing_cusids - target_cusids

        records_to_update = []
        for cid in to_deactivate_ids:
            obj = existing_map[cid]
            obj.fc_status = 'F'
            obj.fn_userinput = actor_id
            records_to_update.append(obj)

        for cid in to_activate_ids:
            obj = existing_map[cid]
            obj.fc_status = 'T'
            obj.fn_userinput = actor_id
            records_to_update.append(obj)

        records_to_create = [
            TAksesusercust(
                fn_userid=target_user_id,
                fn_cusid=cid,
                fc_status='T',
                fn_userinput=actor_id
            ) for cid in to_insert_ids
        ]

        if not dry_run:
            if records_to_update:
                TAksesusercust.objects.using(dbPRS).bulk_update(records_to_update, ['fc_status', 'fn_userinput'])
            if records_to_create:
                TAksesusercust.objects.using(dbPRS).bulk_create(records_to_create)

    def bulk_update_employee_hold(self, request, decoded_payload):
        """
        Method efisien untuk update status hold User (UserTb) & Employee (EmployeesTb) secara massal.
        """
        try:
            data_list = request.data.get('data', [])
            if not data_list:
                return False, "Payload data kosong.", {}

            actor_id = decoded_payload.get('userid') or request.data.get('userid') or 1
            client_ip = _get_client_ip(request)
            current_time = timezone.now()

            # Extract ID yang dikirim
            user_ids = [item['userid'] for item in data_list if item.get('userid')]
            if not user_ids:
                return False, "Tidak ada userid valid yang dikirim.", {}

            # Fetch data eksisting dalam 1 query per tabel
            emp_qs = EmployeesTb.objects.using(dbPRS).filter(fn_userid__in=user_ids)
            emp_objs_map = {emp.fn_userid: emp for emp in emp_qs}
            user_qs = UserTb.objects.using(dbPRS).filter(fn_userid__in=user_ids)
            user_objs_map = {usr.fn_userid: usr for usr in user_qs}

            users_to_update = []
            hs_users_to_create = []
            
            emps_to_update = []
            hs_emps_to_create = []

            for item in data_list:
                uid = item.get('userid')
                if not uid:
                    continue

                hold_u = item.get('fc_hold_u')
                hold_e = item.get('fc_hold_e')

                # 1. Process UserTb & Audit Log
                if hold_u and uid in user_objs_map:
                    u_obj = user_objs_map[uid]
                    if u_obj.fc_hold != hold_u:
                        u_obj.fc_hold = hold_u
                        users_to_update.append(u_obj)
                        
                        hs_users_to_create.append(HsUserTb(
                            fn_userid=u_obj.fn_userid,
                            fv_username=u_obj.fv_username,
                            fd_expireddate=u_obj.fd_expireddate,
                            fc_hold=u_obj.fc_hold,
                            fc_changepassword=u_obj.fc_changepassword,
                            fv_desc=u_obj.fv_desc,
                            fv_password_web=u_obj.fv_password_web,
                            fv_email=u_obj.fv_email,
                            fn_counter=u_obj.fn_counter,
                            fn_userlevel=u_obj.fn_userlevel,
                            fn_whid=u_obj.fn_whid,
                            fn_userinput=u_obj.fn_userinput,
                            fd_dateinput=u_obj.fd_dateinput,
                            fv_ipinput=u_obj.fv_ipinput,
                            fn_userupdate=actor_id,
                            fd_dateupdate=current_time,
                            fv_ipupdate=client_ip,
                            fc_action="U"
                        ))

                # 2. Process EmployeesTb & Audit Log
                if hold_e and uid in emp_objs_map:
                    e_obj = emp_objs_map[uid]
                    if e_obj.fc_hold != hold_e:
                        e_obj.fc_hold = hold_e
                        e_obj.fn_userinput = actor_id
                        emps_to_update.append(e_obj)

                        hs_emps_to_create.append(HsEmployeesTb(
                            fn_userid=e_obj.fn_userid,
                            fn_idemployees=e_obj.fn_idemployees,
                            fv_alias=e_obj.fv_alias,
                            fc_codeemployees=e_obj.fc_codeemployees,
                            fv_nameemployees=e_obj.fv_nameemployees,
                            fv_npwp=e_obj.fv_npwp,
                            fv_nip=e_obj.fv_nip,
                            fv_nik=e_obj.fv_nik,
                            fv_address=e_obj.fv_address,
                            fd_datein=e_obj.fd_datein,
                            fd_dateout=e_obj.fd_dateout,
                            fc_staff=e_obj.fc_staff,
                            fn_userinput=e_obj.fn_userinput,
                            fd_dateinput=e_obj.fd_dateinput,
                            fv_ipinput=e_obj.fv_ipinput,
                            fv_rt=e_obj.fv_rt,
                            fv_rw=e_obj.fv_rw,
                            fv_nomorrekbca=e_obj.fv_nomorrekbca,
                            fv_jeniskelamin=e_obj.fv_jeniskelamin,
                            fd_tgllahir=e_obj.fd_tgllahir,
                            fv_nomorKK=e_obj.fv_nomorKK,
                            fn_kotalahir=e_obj.fn_kotalahir,
                            fv_pendidikanakhir=e_obj.fv_pendidikanakhir,
                            fv_email=e_obj.fv_email,
                            fc_hold=e_obj.fc_hold,
                            fc_action='U',
                            fn_userupdate=actor_id,
                            fd_dateupdate=current_time
                        ))

            # Eksekusi Bulk Database Operations dalam Atomic Transaction
            with transaction.atomic(using=dbPRS):
                if users_to_update:
                    UserTb.objects.using(dbPRS).bulk_update(users_to_update, ['fc_hold'])
                    HsUserTb.objects.using(dbPRS).bulk_create(hs_users_to_create)

                if emps_to_update:
                    EmployeesTb.objects.using(dbPRS).bulk_update(emps_to_update, ['fc_hold', 'fn_userinput'])
                    HsEmployeesTb.objects.using(dbPRS).bulk_create(hs_emps_to_create)

            return True, f"Berhasil memperbarui {len(users_to_update)} user & {len(emps_to_update)} employee.", {}

        except Exception as e:
            raise e

        
class EmployeeMasterDeleteView(APIView):
    """
    Menangani penonaktifan karyawan massal (HOLD = 'T').
    Menggunakan context manager 'transaction.atomic' untuk replikasi DB::transaction Laravel.
    """
    def post(self, request, level):
        tanggal_input = request.data.get('tanggal')
        hold_ids_raw = request.data.get('hold', '')
        
        if level == 'manager' or not hold_ids_raw:
            messages.warning(request, "Aksi tidak valid atau data kosong")
            return redirect(f"employee-master", level=level)

        # Ubah string comma-separated '1,2,3' menjadi list of integers [1, 2, 3] via numpy/python
        id_list = [int(x) for x in hold_ids_raw.split(',') if x.strip().isdigit()]
        formatted_date = datetime.strptime(tanggal_input, '%Y-%m-%d').strftime('%Y-%m-%d 00:00:00') if tanggal_input else datetime.now().strftime('%Y-%m-%d 00:00:00')

        try:
            # GAYA DJANGO: Membungkus transaksi database dalam blok ACID atomic
            with transaction.atomic(using='default'), transaction.atomic(using='gis'):
                
                # Gunakan koneksi database kustom 'gis' untuk logging history data lama
                with connections['gis'].cursor() as cursor_gis:
                    for emp_id in id_list:
                        # 1. Ambil data asli employees_tb untuk dipindah ke history (hs_employees_tb)
                        cursor_gis.execute("SELECT * FROM employees_tb WHERE fn_idemployees = %s", [emp_id])
                        emp_data = cursor_gis.fetchone() # returns dict jika row_factory diset, atau tuple
                        
                        if emp_data:
                            # Menggunakan Pandas untuk mempermudah manipulasi data log sebelum di-insert ke history
                            df_hist_emp = pd.DataFrame([emp_data])
                            df_hist_emp['fn_userupdate'] = request.session.get('userid', 0)
                            df_hist_emp['fd_dateupdate'] = datetime.now()
                            df_hist_emp['fv_ipupdate'] = request.META.get('REMOTE_ADDR', '127.0.0.1')
                            df_hist_emp['fc_action'] = 'D'
                            
                            # Jalankan raw insert history employee (Metode manual atau bulk)
                            # ... kueri insert hs_employees_tb ...

                            # 2. Ambil data user_tb untuk dicatat ke history (hs_user_tb)
                            u_id = int(df_hist_emp['fn_userid'].iloc[0])
                            cursor_gis.execute("SELECT * FROM user_tb WHERE fn_userid = %s", [u_id])
                            user_data = cursor_gis.fetchone()
                            
                            if user_data:
                                df_hist_user = pd.DataFrame([user_data])
                                df_hist_user['fn_userupdate'] = request.session.get('userid', 0)
                                df_hist_user['fd_dateupdate'] = datetime.now()
                                df_hist_user['fv_ipupdate'] = request.META.get('REMOTE_HOST', '127.0.0.1')
                                df_hist_user['fc_action'] = 'D'
                                # ... kueri insert hs_user_tb ...

                # 3. BULK UPDATE STATUS HOLD KE 'T' (Eksekusi Masal Berbasis Set List)
                placeholders = ', '.join(['%s'] * len(id_list))
                
                # Ambil daftar fn_userid yang terkena dampak masal lewat query IN
                query_get_users = f"SELECT fn_userid FROM employees_tb WHERE fn_idemployees IN ({placeholders})"
                affected_users = [row['fn_userid'] for row in DatabaseConnector.execute_raw_sql(query_get_users, params=id_list, using='gis')]
                user_placeholders = ', '.join(['%s'] * len(affected_users))

                # Update Database 'gis'
                query_upd_emp_gis = f"UPDATE employees_tb SET fc_hold = 'T', fd_dateout = %s WHERE fn_idemployees IN ({placeholders})"
                DatabaseConnector.execute_raw_sql(query_upd_emp_gis, params=[formatted_date] + id_list, using='gis', return_dict=False)
                
                if affected_users:
                    query_upd_usr_gis = f"UPDATE user_tb SET fc_hold = 'T' WHERE fn_userid IN ({user_placeholders})"
                    DatabaseConnector.execute_raw_sql(query_upd_usr_gis, params=affected_users, using='gis', return_dict=False)

                # Update Database Default Local ('default')
                query_upd_emp_local = f"UPDATE employees_tb SET fc_hold = 'T', fd_dateout = %s WHERE fn_idemployees IN ({placeholders})"
                DatabaseConnector.execute_raw_sql(query_upd_emp_local, params=[formatted_date] + id_list, using='default', return_dict=False)
                
                if affected_users:
                    query_upd_usr_local = f"UPDATE user_tb SET fc_hold = 'T' WHERE fn_userid IN ({user_placeholders})"
                    DatabaseConnector.execute_raw_sql(query_upd_usr_local, params=affected_users, using='default', return_dict=False)

            messages.success(request, "Karyawan berhasil dinon-aktifkan masal.")
        except Exception as e:
            messages.error(request, f"Gagal menonaktifkan: {str(e)}")
            
        return redirect(f"/employee/{level}/master")


class EmployeeReportView(APIView):
    """Menangani riwayat klaim, seragam, debit (Kasbon), dan Ringkasan Absensi AJAX"""
    
    def get(self, request, level, report_type):
        # 1. AJAX Handler untuk Ringkasan Kasbon (Debit Summary)
        if report_type == 'debit' and request.headers.get('x-requested-with') == 'XMLHttpRequest':
            # Contoh query kasbon karyawan
            raw_debit = DatabaseConnector.execute_raw_sql("SELECT * FROM debit_summary_tb")
            df = pd.DataFrame(raw_debit) if raw_debit else pd.DataFrame()
            return Response(df.to_dict(orient='records') if not df.empty else [])

        # 2. AJAX Handler untuk Ringkasan Absensi (Attendance Summary)
        if report_type == 'attendance' and request.headers.get('x-requested-with') == 'XMLHttpRequest':
            # Proses filtering berbasis data analyst dengan parameter request
            params = list(request.GET.values())
            raw_att = DatabaseConnector.execute_raw_sql("SELECT * FROM attendance_tb WHERE fc_jenis = %s", params=params[:1])
            return Response(raw_att if raw_att else [])

        # 3. Render HTML Default View berdasarkan jenis sub-report
        titles = {
            'claim': 'History Claim',
            'uniform': 'History Uniform',
            'debit': 'History Debit',
            'attendance': 'Report Absensi SPG/B' if level == 'operator' else f'Report Absensi {level.capitalize()}'
        }
        
        context = {
            'level': level,
            'title': titles.get(report_type, '')
        }
        return render(request, f'desktop/employee/{report_type}.html', context)