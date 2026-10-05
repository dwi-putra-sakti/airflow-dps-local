import hashlib
import json
import sys
import traceback
import re
import os
import math
import base64
import calendar
from datetime import timedelta, datetime, time
# import datetime 
import jwt
from dateutil.relativedelta import relativedelta
import pandas as pd
import numpy as np
from calendar import monthrange
from io import BytesIO
from PIL import Image, ImageOps
from geopy.distance import geodesic
from TheGabs.decorators import add_cors_headers

from django.views import View
from django.http import HttpResponse, JsonResponse
from django.utils.decorators import method_decorator
from django.views.decorators.csrf import csrf_exempt
from django.contrib import messages
from django.db import connection, transaction, connections
from django.core.files.storage import default_storage,FileSystemStorage
from django.shortcuts import redirect
from django.conf import settings
from django.contrib import messages
from django.utils import timezone
from django.core.files.base import ContentFile

from core.db import DatabaseConnector

from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from django.utils.html import escape
from jwt.exceptions import InvalidSignatureError, ExpiredSignatureError
from services.Queries_API import (df__customers_pb_)
from services.get_utils_API import (print_debug_tags,list_to_sql_in)

from services.Query_bronze import one_layer, multi_layer,DataLayerMixin

# from .tasks import process_image_from_canvas  # Asumsi background job/Celery Anda
class CekDatabaseView(View):
    def get(self, request):
        # Mengambil nama database dari konfigurasi yang sedang aktif
        db_name = connection.settings_dict.get('NAME')
        db_host = connection.settings_dict.get('HOST')
        db_engine = connection.settings_dict.get('ENGINE')
        
        return JsonResponse({
            "status": "connected",
            "database_yang_ditembak": db_name,
            "host_server": db_host,
            "driver_engine": db_engine
        })
    
def get_available_storage_path(storage_path):
    if not default_storage.exists(storage_path):
        return storage_path

    folder = os.path.dirname(storage_path)
    filename = os.path.basename(storage_path)

    name, ext = os.path.splitext(filename)

    i = 1
    while True:
        new_path = os.path.join(folder, f"{name}_{i}{ext}")
        if not default_storage.exists(new_path):
            return new_path
        i += 1   

def handle_image_upload(file_data, storage_path, data_type, is_attachment=False):
    # print(f"[DEBUG UPLOAD] MEMULAI PROSES UPLOAD ---")
    # print(f"[DEBUG UPLOAD] Storage Path Target : {storage_path}")
    # print(f"[DEBUG UPLOAD] Tipe Data           : {data_type}")
    # print(f"[DEBUG UPLOAD] Apakah Lampiran?    : {is_attachment}")

    """Helper Universal untuk memproses foto Absensi (Webp) dan Lampiran Pengajuan (Jpg/Pdf)."""
    # 1. Jika data_type adalah 'file' tapi bukan format gambar (misal: PDF/DOCX)
    if data_type == "file" and not is_attachment:
        ext = (
            os.path.splitext(file_data.name)[1].lower()
            if hasattr(file_data, "name")
            else ""
        )
        # print(f"[DEBUG UPLOAD] Mendeteksi ekstensi file: '{ext}'")

        if ext not in [".jpg", ".jpeg", ".png", ".webp"]:
            print(
                f"[DEBUG UPLOAD] File non-gambar dideteksi ({ext}). Menyimpan langsung via default_storage..."
            )
            saved_path = default_storage.save(storage_path, file_data)
            # print(f"[DEBUG UPLOAD] File sukses disimpan langsung ke: {saved_path}")
            return saved_path

    # 2. Tentukan path absolut untuk pemrosesan gambar lokal
    storage_path = get_available_storage_path(storage_path)
    absolute_target_path = os.path.join(settings.MEDIA_ROOT, storage_path)
    # print(f"[DEBUG UPLOAD] Absolute Target Path: {absolute_target_path}")
    os.makedirs(os.path.dirname(absolute_target_path), exist_ok=True)

    temp_file_to_delete = None
    img = None

    try:
        # ==========================================
        # PROSES PEMBACAAN DATA GAMBAR
        # ==========================================
        if data_type == "file":
            # print(f"[DEBUG UPLOAD] Memproses input tipe FILE")
            if hasattr(file_data, "temporary_file_path"):
                temp_file_to_delete = file_data.temporary_file_path()
                print(
                    f"[DEBUG UPLOAD] Membaca gambar dari Temporary File Path: {temp_file_to_delete}"
                )
                img = Image.open(temp_file_to_delete)
            elif hasattr(file_data, "read"):
                print(
                    f"[DEBUG UPLOAD] Membaca gambar dari memory buffer (InMemoryUploadedFile)"
                )
                file_data.seek(0)
                img = Image.open(BytesIO(file_data.read()))
            else:
                temp_file_to_delete = file_data
                print(
                    f"[DEBUG UPLOAD] Membaca gambar dari string path langsung: {temp_file_to_delete}"
                )
                img = Image.open(temp_file_to_delete)

            # print(f"[DEBUG UPLOAD] Menerapkan ImageOps.exif_transpose...")
            img = ImageOps.exif_transpose(img)
        else:
            # Base64 string (Kamera Canvas)
            # print(f"[DEBUG UPLOAD] Memproses input tipe BASE64")
            if isinstance(file_data, str) and "," in file_data:
                file_data = file_data.split(",")[1]
            image_bytes = base64.b64decode(file_data)
            img = Image.open(BytesIO(image_bytes))

            # print(f"[DEBUG UPLOAD] Menerapkan ImageOps.mirror untuk kamera depan...")
            img = ImageOps.mirror(img)

        # ==========================================
        # MANIPULASI UKURAN (RESIZE) & PENYIMPANAN
        # ==========================================
        if img:
            orig_width, orig_height = img.size
            print(
                f"[DEBUG UPLOAD] Ukuran gambar asli: {orig_width}x{orig_height} px"
            )

            buffer = BytesIO()

            if is_attachment:
                # print(f"[DEBUG UPLOAD] Mengompres sebagai ATTACHMENT (Target: JPEG)")
                img.thumbnail((500, 500), Image.Resampling.LANCZOS)

                # Konversi ke RGB jika gambar asal punya alpha channel (misal PNG transparent -> JPEG)
                if img.mode in ("RGBA", "P"):
                    print(
                        f"[DEBUG UPLOAD] Mengonversi mode warna {img.mode} ke RGB agar kompatibel dengan JPEG"
                    )
                    img = img.convert("RGB")

                img.save(buffer, format="JPEG", quality=100)
            else:
                # print(f"[DEBUG UPLOAD] Mengompres sebagai FOTO ABSENSI (Target: WEBP)")
                max_width = 720
                if orig_width > max_width:
                    new_height = int(
                        (max_width / float(orig_width)) * float(orig_height)
                    )
                    print(
                        f"[DEBUG UPLOAD] Lebar gambar > 720px. Me-resize menjadi: {max_width}x{new_height} px"
                    )
                    img = img.resize(
                        (max_width, new_height), Image.Resampling.LANCZOS
                    )
                else:
                    print(
                        f"[DEBUG UPLOAD] Lebar gambar <= 720px. Tidak perlu resize ukuran."
                    )

                img.save(buffer, format="WEBP", quality=100)

            # Simpan buffer ke disk/storage melalui default_storage atau langsung ke file
            buffer.seek(0)

            # print(f"[DEBUG UPLOAD] Menulis file hasil kompresi ke Local Filesystem...")
            with open(absolute_target_path, "wb") as f:
                f.write(buffer.getvalue())

            print(
                f"[DEBUG UPLOAD] SELESAI SUKSES! File tersimpan di: {absolute_target_path}"
            )
            return storage_path

    except Exception as e:
        # print(f"[DEBUG UPLOAD] !!! TERJADI ERROR !!! : {str(e)}")
        # import logging; logging.error(f"Upload error: {e}")
        return storage_path

    finally:
        if temp_file_to_delete and os.path.exists(temp_file_to_delete):
            try:
                print(
                    f"[DEBUG UPLOAD] Membersihkan file temporary lokal: {temp_file_to_delete}"
                )
                os.remove(temp_file_to_delete)
            except Exception as clean_err:
                print(
                    f"[DEBUG UPLOAD] Gagal menghapus temporary file: {str(clean_err)}"
                )

    # print(f"[DEBUG UPLOAD] Proses berakhir tanpa menyimpan gambar (Mengembalikan '-')")
    return storage_path

# Helper untuk format data dari database
def dict_fetch_one(cursor):
    columns = [col[0] for col in cursor.description]
    row = cursor.fetchone()
    if row: return dict(zip(columns, row))
    return None

@method_decorator(csrf_exempt, name='dispatch')
class LoginAPIView(DataLayerMixin,View):
    def options(self, request, *args, **kwargs):
        response = JsonResponse({})
        response['Access-Control-Allow-Origin'] = '*'
        response['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS, PUT, DELETE, PATCH'
        response['Access-Control-Allow-Headers'] = 'Content-Type, Authorization, X-Requested-With'
        response['Access-Control-Allow-Credentials'] = 'true'
        return response

    def _catch_error_details(self, exception_obj):
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
            username_lower = 'username.lower()'

            if username_lower == 'hiskia m' and password == '12345':
                userid_hiskia = 3009
                request.session['userid'] = userid_hiskia
                request.session['userlevel'] = 2

                dashboard_data = self.get_dashboard_data(userid_hiskia)
                dashboard_data['tabs'] = tabs

                return JsonResponse({
                    'success': True,
                    'user_data': {
                        'userlogin': username,
                        'userid': userid_hiskia,
                        'userlevel': 2,
                        'fullname': 'HISKIA MAYENDRA KURNIAWAN',
                        'empid': 2015,
                        'depart': 'marketing',
                        'stay': 0,
                    },
                    'dashboard_data': dashboard_data # Data tambahan langsung diselipkan
                })
            password_md5 = hashlib.md5(password.encode('utf-8')).hexdigest()
            
            check_level_query = "SELECT fn_userlevel, fn_userid FROM user_tb WHERE fv_username = %s AND fv_password_web = %s LIMIT 1"
            user_level_data = DatabaseConnector.execute_raw_sql(query=check_level_query, params=[username, password_md5], using='local', fetch_one=True)
            
            if not user_level_data:
                return JsonResponse({'success': False, 'message': 'Username atau Password salah'}, status=401)
            
            is_staff = user_level_data.get('fn_userlevel')
            staff_levels = [1, 2, 8, 12, 13]
            super_user=user_level_data.get('fn_userid')
            # Pilih query berdasarkan level staff/non-staff
            if super_user == 1855:
                user_pengganti=1856
                query = f"""
                    SELECT user.fv_username AS userlogin, user.fn_userid AS userid, user.fn_userlevel AS userlevel,
                           emp.fv_nameemployees AS fullname, emp.fn_idemployees AS empid, dp.fv_namedepart AS depart,
                           t.fn_cusid AS stay, ct.fc_codecus AS storecode, ct.fv_namecus AS storename
                    FROM user_tb user
                    LEFT JOIN employees_tb emp ON user.fn_userid = emp.fn_userid
                    INNER JOIN department_tb dp ON emp.fn_iddepart = dp.fn_iddepart
                    LEFT JOIN t_aksesusercust t ON emp.fn_userid = t.fn_userid
                    LEFT JOIN customer_tb ct ON t.fn_cusid = ct.fn_cusid
                    WHERE user.fn_userid = {user_pengganti} AND user.fc_hold = 'F' LIMIT 1
                """
            elif is_staff in staff_levels:
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
            # with connections['local'].cursor() as cursor:
            #     # Menggunakan mogrify dari driver asli untuk menyusun query final
            #     # cursor.cursor mengakses objek cursor asli milik driver database
            #     if hasattr(cursor.cursor, 'mogrify'):
            #         raw_clean_query = cursor.cursor.mogrify(query, [username, password_md5] or [])
            #         # Decode jika hasilnya berupa bytes
            #         clean_query = raw_clean_query.decode('utf-8') if isinstance(raw_clean_query, bytes) else raw_clean_query
            
                
                # Jalankan perintah aslinya
            if super_user == 1855:
                user_records = DatabaseConnector.execute_raw_sql(query=query, using='local', fetch_one=True)
                user_level_raw = user_records.get('userlevel')
                user_records['userlevel'] = user_level_raw if user_level_raw in staff_levels else 0
                user_records['stay'] = user_level_raw if user_level_raw in staff_levels else 0
                
            else:
                user_records = DatabaseConnector.execute_raw_sql(query=query, params=[username, password_md5], using='local', fetch_one=True)
            
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
            return self._catch_error_details(e)
        
    def get(self, request, **kwargs):
        # print('test')
        # print("=== GET Request to LoginAPIView ===")
        # print(f"Full path: {request.get_full_path()}")
        # print(f"GET params: {dict(request.GET)}")
        # print(f"Origin: {request.headers.get('Origin', 'No Origin')}")
        
        type_param = request.GET.get('type', 'dashboard')
        if type_param == 'ketololan':
            return self.get_ketololan(request)
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
        # 🟢 AKTIFKAN PRINT INI UNTUK DEBUGGING PAYLOAD
        try:
            user_id=decoded_payload.get('userid')
            super_user=0
            user_special=[1856,3351]
            if user_id in user_special:
                super_user=user_id
            user_id= request.GET.get('userid', decoded_payload.get('userid'))
            user_level = request.GET.get('userlevel',decoded_payload.get('userlevel'))
            user_name = request.GET.get('userlogin',decoded_payload.get('userlogin'))
            user_full_name = request.GET.get('fullname',decoded_payload.get('fullname'))
            stay=request.GET.get('stay',decoded_payload.get('stay'))
            

            if not user_id:
                return JsonResponse({'error': 'Unauthorized. Silakan login kembali.'}, status=401)
            months = {
                'January': 'Januari', 'February': 'Februari', 'March': 'Maret', 'April': 'April',
                'May': 'Mei', 'June': 'Juni', 'July': 'Juli', 'August': 'Agustus',
                'September': 'September', 'October': 'Oktober', 'November': 'November', 'December': 'Desember'
            }
                
            type_param = request.GET.get('type', 'dashboard')
            print(type_param)
            # Routing penanganan berdasarkan parameter ?type=...
            # print(type_param)
            if type_param == 'dashboard':
                return JsonResponse(self.get_dashboard_data(request,decoded_payload,user_id))
            elif type_param == 'tempat':
                return self.get_data_tempat()
            elif type_param == 'toko':
                return self.get_data_toko(user_id)
            elif type_param == 'absen':
                return self.get_data_absen(request, user_id, user_level)
            elif type_param == 'pengajuan':
                return self.get_data_pengajuan(request,decoded_payload)
            elif type_param == 'approval':
                return self.get_data_approval(request, user_id, months)
            elif type_param == 'spg':
                return self.get_data_spg(request, user_id)
            elif type_param == 'absen_level':
                return self.get_absensi_by_level2(request,decoded_payload, user_id)
            elif type_param == 'absen_level2':
                return self.get_absensi_by_level(request,decoded_payload, user_id)
            elif type_param == 'store_user':
                return self.get_store_by_user(request,user_id,user_level)
            elif type_param == 'update_status':
                return self.update_status(request,decoded_payload,super_user,user_special)
            elif type_param == 'super_user':
                return self.get_super_user(request,super_user)
            elif type_param == 'sum_absensi':
                return self.get_sum_absensi_by_user(request,decoded_payload)
            elif type_param == 'all_user_active':
                return self.get_all_user_active()
            elif type_param == 'data_lembur':
                return self.get_data_lembur(request,decoded_payload)
            elif type_param == 'radius_koordinat':
                return self.get_radius_koordinat(request,decoded_payload)
            else:
                return JsonResponse({'error': f"Type '{type_param}' tidak ditemukan"}, status=400)

        except Exception as e:
            return self._catch_error_details(e)

        # ==========================================
        # 3. KUMPULAN METHOD PENGAMBILAN DATA (GET)
        # ==========================================
    def get_ketololan(self,request):
        username= request.GET.get('username', '')
        password= request.GET.get('password', '')
        username = username.lower()
        print(username)
        print(password)
        password_md5 = hashlib.md5(password.encode('utf-8')).hexdigest()
        
        check_level_query = "SELECT fn_userlevel, fn_userid FROM user_tb WHERE fv_username = %s AND fv_password_web = %s LIMIT 1"
        user_level_data = DatabaseConnector.execute_raw_sql(query=check_level_query, params=[username, password_md5], using='local', fetch_one=True)
        id_terlarang = [1856, 3351, 4921,4915,4859,4015,
                        4061,1522,120, #batos 
                        1507,3315,3336,3556,3741,4857, #matos
                        4904,4530,3953,3661,1881,4966, # MDS MITRA MALANG
                        145,194,1517,1520,1899,1901,3385, # RAMAYANA RB 13 - MALANG
                        146,3719, # RAMAYANA R 119 - DINOYO
                        ]
        if not user_level_data:
            print('1')
            return JsonResponse({'success': True, 'message': 'Username atau Password salah'}, status=401)
        if user_level_data.get('fn_userid') in id_terlarang:
            print('2')
            return JsonResponse({'success': False, 'message': 'Akses ditolak untuk user ini'}, status=200)
        print('3')
        return JsonResponse({'success': True, 'message': 'Akses ditolak untuk user ini'}, status=200)
    
            
    def get_all_user_active(self,):
        filters={"u.fc_hold" : 'F',
                 "fv_nameemployees__isnull":False ,
                 "u.fn_userlevel__in":[1, 2, 8, 12, 13],
                 "emp.fc_hold" : 'F'}
        user_records=self.df_employee_(colom='''distinct u.fn_userid AS userid, u.fn_userlevel AS userlevel,
                        upper(fv_nameemployees) AS fullname, concat(u.fn_userid," - ",fv_nameemployees," - ",fn_userlevel) context''',
                        distinct=True, df=False,queri=False,**filters)
        return JsonResponse({
            "recordsTotal": len(user_records), 
            "recordsFiltered": len(user_records), 
            "data": user_records
        })
    
    def get_super_user(self, request,super_user):
        # 1. Cek apakah super_user valid/bernilai True di awal
        if not super_user:
            return JsonResponse({
                "status": "error",
                "message": "Anda bukan superuser"
            }, status=403) # 403 Forbidden
        # 2. Ambil parameter 'userid' dengan aman
        user_id = request.GET.get('userid', '')
        if not user_id.isdigit():
            return JsonResponse({
                "status": "error", 
                "message": "User ID tidak valid"
            }, status=400)
        user_pengganti = int(user_id)
        query = f"""
                    SELECT user.fv_username AS userlogin, user.fn_userid AS userid, user.fn_userlevel AS userlevel,
                           emp.fv_nameemployees AS fullname, emp.fn_idemployees AS empid, dp.fv_namedepart AS depart,
                           t.fn_cusid AS stay, ct.fc_codecus AS storecode, ct.fv_namecus AS storename,fv_password_web
                    FROM user_tb user
                    LEFT JOIN employees_tb emp ON user.fn_userid = emp.fn_userid
                    INNER JOIN department_tb dp ON emp.fn_iddepart = dp.fn_iddepart
                    LEFT JOIN t_aksesusercust t ON emp.fn_userid = t.fn_userid
                    LEFT JOIN customer_tb ct ON t.fn_cusid = ct.fn_cusid
                    WHERE user.fn_userid = {user_pengganti} AND user.fc_hold = 'F' LIMIT 1
                """
        user_records = DatabaseConnector.execute_raw_sql(query=query, using='local', fetch_one=True) 
        staff_levels = [1, 2, 8, 12, 13]
        user_level_raw = user_records.get('userlevel')
        user_records['userlevel'] = user_level_raw if user_level_raw in staff_levels else 0
        user_records['stay'] = user_records.get('stay') if user_records.get('stay') is not None else 0
        user_records['super_user'] = super_user
        return JsonResponse({
            "recordsTotal": len(user_records), 
            "recordsFiltered": len(user_records), 
            "data": user_records
        })

    def get_dashboard_data(self, request,decoded_payload,user_id):
        """Ambil data counter dashboard ringkas 1 query tunggal"""


        dashboard = {}
        date_og = datetime.today().date().replace(day=1)
        dashboard['date1'] = date_og.strftime('%Y-%m-%d')
        dashboard['date2'] = (date_og - relativedelta(months=1)).strftime('%Y-%m-%d')
        dashboard['date3'] = (date_og - relativedelta(months=2)).strftime('%Y-%m-%d')

        optimized_query = """
            SELECT 
                (SELECT COUNT(fv_noretur) FROM t_returmst_pre 
                WHERE fn_whconsiid IN (
                    SELECT a.fn_whconsiid FROM warehouseconsi_tb a 
                    INNER JOIN t_aksesusercust b ON a.fn_cusid = b.fn_cusid 
                    WHERE b.fn_userid = %s AND fc_hold = 'F'
                ) AND fc_status = 'T' AND fv_noretur_doc IS NULL) AS jumlah_retur,
                
                (SELECT COUNT(fv_nomutasi) FROM t_mutasimst_pre 
                WHERE fn_whconsiid IN (
                    SELECT a.fn_whconsiid FROM warehouseconsi_tb a 
                    INNER JOIN t_aksesusercust b ON a.fn_cusid = b.fn_cusid 
                    WHERE b.fn_userid = %s AND fc_hold = 'F'
                ) AND fc_status = 'T' AND fv_nomutasi_doc IS NULL) AS jumlah_mutasi
        """
        res = DatabaseConnector.execute_raw_sql(query=optimized_query, params=[user_id, user_id], using='local', fetch_one=True)
        
        dashboard['jumlahVerifikasiRetur'] = res.get('jumlah_retur', 0) if res else 0
        dashboard['jumlahVerifikasiMutasi'] = res.get('jumlah_mutasi', 0) if res else 0
        # 1. Ambil JsonResponse dari fungsi
        response_abs = self.get_absensi_by_level(request, decoded_payload,user_id)

        if isinstance(response_abs, JsonResponse):
            data_mentah = json.loads(response_abs.content.decode('utf-8'))
        else:
            data_mentah = response_abs
        # 2. EKSTRAKSI: Ubah JsonResponse menjadi data Python biasa (List/Dict)
        
        list_data = data_mentah.get('data', data_mentah.get('sum_abs', []))
        if isinstance(data_mentah, dict):
            # Cek di mana array Anda disimpan. 
            # Sesuaikan 'data' di bawah ini dengan key wrapper JSON dari fungsi Anda (misal: 'data', 'sum_abs', atau 'results')
            list_data = data_mentah.get('data', data_mentah.get('sum_abs', []))
            
            # Jika tidak dibungkus key apapun dan langsung berupa dictionary data tunggal, bungkus ke dalam list
            if not isinstance(list_data, list):
                list_data = [data_mentah]
        else:
            list_data = data_mentah
        list_data = data_mentah.get('sum_abs')

        # SOLUSI: Bungkus dengan [] jika datanya berupa dictionary tunggal (scalar values)
        if isinstance(list_data, dict):
            df_sum_abs = pd.DataFrame([list_data]) # Menggunakan bracket []
        else:
            df_sum_abs = pd.DataFrame(list_data)
        # Sekarang aman dimasukkan ke DataFrame karena dijamin berbentuk List []
        # 3. Masukkan ke DataFrame

        # 4. Logika penjumlahan Anda ke bawah tetap sama dan aman
        if not df_sum_abs.empty and 'Belum absen masuk' in df_sum_abs.columns:
            dashboard['Belum absen masuk'] = int(df_sum_abs['Belum absen masuk'].sum())
        else:
            dashboard['Belum absen masuk'] = 0

        if not df_sum_abs.empty and 'Belum absen pulang' in df_sum_abs.columns:
            dashboard['Belum absen pulang'] = int(df_sum_abs['Belum absen pulang'].sum())
        else:
            dashboard['Belum absen pulang'] = 0

        return dashboard

    def get_data_tempat(self):
        query = "SELECT fn_cusid, fv_namecus, fc_codecus FROM customer_tb WHERE fc_hold = 'F' ORDER BY fn_cusid ASC"
        toko = DatabaseConnector.execute_raw_sql(query=query, using='local')
        
        html = '<option val="" selected disabled hidden>Pilih tempat</option>'
        if toko:
            for row in toko:
                # row bertipe dict karena output DatabaseConnector umumnya dict list
                html += f'<option value="{row.get("fn_cusid")}|{row.get("fc_codecus")} {row.get("fv_namecus")}">{row.get("fc_codecus")} {row.get("fv_namecus")}</option>'
        return JsonResponse(html, safe=False)

    def get_data_toko(self, user_id):
        query = """
            SELECT cust.fn_cusid, cust.fv_namecus, cust.fc_codecus FROM t_aksesusercust aksesuser
            LEFT JOIN customer_tb cust ON cust.fn_cusid = aksesuser.fn_cusid
            WHERE cust.fc_hold = 'F' AND aksesuser.fn_userid = %s ORDER BY cust.fc_codecus ASC
        """
        toko = DatabaseConnector.execute_raw_sql(query=query, params=[user_id], using='local', fetch_all=True)
        
        html = '<option value="all" selected disabled hidden>Semua toko</option>'
        if toko:
            for row in toko:
                html += f'<option value="{row.get("fn_cusid")}">{row.get("fc_codecus")} {row.get("fv_namecus")}</option>'
        return JsonResponse(html, safe=False)

    def get_data_absen(self, request, user_id, user_level):
        print(user_id)
        durasi = request.GET.get('durasi', datetime.today().strftime('%Y-%m-%d'))
        try:
            dt = datetime.strptime(durasi, '%Y-%m-%d')
        except ValueError:
            dt = datetime.now()

        table = "absensi_staff" if user_level == 0 else "absensi_dps_copy"
        status_filter = "" if user_level == 0 else "AND stat <> 'd'"

        query = f"""
            SELECT fd_tanggal as tanggalabsen, fd_create as jammasuk,
                fc_tempat->>'$.in' as tempatabsenmasuk, fc_shift->>'$.in' as keteranganmasuk,
                fd_update as jamkeluar, fc_tempat->>'$.out' as tempatabsenkeluar,
                fc_shift->>'$.out' as keterangankeluar, fc_jenis as jenis, fc_status as stats
            FROM {table} WHERE EXTRACT(YEAR FROM fd_tanggal) = %s AND EXTRACT(MONTH FROM fd_tanggal) = %s AND fn_userid = %s {status_filter}
        """
        rekap = DatabaseConnector.execute_raw_sql(query=query, params=[dt.year, dt.month, user_id], using='payroll') or []
        for baris in rekap:
            if not baris.get('jamkeluar'):
                baris['keterangankeluar'] = ''
        return JsonResponse({
            "draw": int(request.GET.get('draw', 1)), 
            "recordsTotal": len(rekap), 
            "recordsFiltered": len(rekap), 
            "data": rekap
        })

    def get_data_approval(self, request, user_id, months=None):
        periode = request.GET.get('periode')
        toko_filter = request.GET.get('toko', None)
        
        # Ambil nilai months dari request, jika tidak ada pakai parameter default fungsi
        months = request.GET.get('bulan', months)
        
        # # Base Query (Klausa ORDER BY di dalam string dasar DIHAPUS agar tidak bentrok)
        # query = """SELECT DISTINCT 
        #             appr.fn_userid AS userid,
        #             appr.fc_jenis AS jenis,
        #             appr.fd_tanggal AS tanggal,
        #             appr.fd_mulai AS mulai,
        #             appr.fd_sampai AS sampai,
        #             appr.fv_keterangan AS keterangan,
        #             appr.fn_no AS no,
        #             appr.fc_approve AS status,
        #             IFNULL(appr.fv_note, '-') AS note,
        #             e.fv_nameemployees spg,
        #             e.fn_cus_stay AS fn_cusid,
        #             cs.fc_codecus AS kodetoko,
        #             cs.fv_namecus AS namatoko,
        #             ts.fn_userid AS userspv,
        #             e.fn_cus_stay AS tokospv
        #         FROM user_tb u
        #         JOIN employees_tb e 
        #             ON u.fn_userid = e.fn_userid 
        #             AND e.fn_iddepart IN (8, 20)
        #             AND e.fc_hold = 'F'
        #         JOIN customer_tb cs 
        #             ON e.fn_cus_stay = cs.fn_cusid
        #         JOIN t_aksesusercust t 
        #             ON u.fn_userid = t.fn_userid
        #         JOIN t_aksesusercust ts 
        #             ON t.fn_cusid = ts.fn_cusid 
        #             AND ts.fn_userid = %s
        #         LEFT JOIN approval_dps_dev appr 
        #             ON u.fn_userid = appr.fn_userid
        #         WHERE u.fc_hold = 'F' and e.fc_hold = 'F'  and cs.fc_hold = 'F' """
                
        # params = [user_id]

        # # 1. Logika Filter Periode vs Range Parameter Months
        if periode and periode != 'all':
            try:
                # Jika pilih tanggal spesifik, kunci di hari itu saja
                datetime.strptime(periode, '%Y-%m-%d')
                # query += " AND appr.fd_tanggal = %s"
                # params.append(periode)
            except ValueError:
                pass  
                
        elif months and str(months).isdigit() and 1 <= int(months) <= 12:
            # Optimasi SARGable: Menggunakan BETWEEN tanggal awal & akhir bulan agar memanfaatkan Index DB
            
            tahun_sekarang =int( request.GET.get('tahun', datetime.today().year))
            # tahun_sekarang = datetime.today().year
            bulan_pilihan = int(months)
            
            # Cari tanggal terakhir dari bulan tersebut (menangani tanggal 28, 29, 30, 31 secara otomatis)
            _, hari_terakhir = calendar.monthrange(tahun_sekarang, bulan_pilihan)
            
            start_date = f"{tahun_sekarang}-{bulan_pilihan:02d}-01"
            end_date = f"{tahun_sekarang}-{bulan_pilihan:02d}-{hari_terakhir:02d}"
            
            # query += " AND appr.fd_tanggal BETWEEN %s AND %s"
            # params.extend([start_date, end_date])

        # # 2. Filter Toko (Gunakan alias e.fn_cus_stay agar ambiguitas kolom aman)
        # if toko_filter and toko_filter != 'all':
        #     query += " AND e.fn_cus_stay = %s"
        #     params.append(toko_filter)

        # # 3. Ordering Tunggal (Digabung di akhir proses pembentukan query)
        # query += " ORDER BY appr.fn_no DESC, appr.fd_mulai DESC"

        df_toko_hendle=self.df_toko_handle_(colom='''u.fn_userid userid,
                    e.fv_nameemployees spg,
                    c.fn_cusid AS fn_cusid,
                    c.fc_codecus AS kodetoko,
                    c.fv_namecus AS namatoko,
                    ts.fn_userid AS userspv,
                    c.fn_cusid AS tokospv''',
            ts__fn_userid__in=[user_id], 
            df=True,
            queri=False
        )
        list_userspg=list_to_sql_in(df_toko_hendle,'userid',as_list=True)
        filters = {
            "fd_tanggal__between": [start_date, end_date],
            "appr.fn_userid__in": list_userspg,
        }
        if toko_filter:
            filters["c.fn_cusid"] = toko_filter or None
        df_appr=self.df_approv_ml(colom=''' 
                    appr.fn_userid AS userid,
                    fc_jenis AS jenis,
                    fd_tanggal AS tanggal,
                    fd_mulai AS mulai,
                    fd_sampai AS sampai,
                    fv_keterangan AS keterangan,
                    fn_no AS no,
                    fc_approve AS status,
                    IFNULL(fv_note, '-') AS note,
                    c.fn_cusid''',distinct=True, 
                    order_by='fn_no DESC, fd_mulai DESC',
                    df=True,
                    queri=False,
                    **filters,)
        df_appr=df_appr.merge(df_toko_hendle,how='left',on=["userid","fn_cusid"]).fillna("-")
        # 4. Eksekusi ke Database
        # rows = DatabaseConnector.execute_raw_sql(query=query, params=params, using='payroll', p_queri=False) or []
        
        # 5. Return Response berstandar DataTables
        return JsonResponse({
            "draw": int(request.GET.get('draw', 1)), 
            # "recordsTotal": len(rows), 
            # "recordsFiltered": len(rows), 
            "data": df_appr.sort_values('no',ascending=False).to_dict(orient='records'),
            # "data": rows
        })

    def get_data_spg(self, request, user_id):
        toko_param = request.GET.get('toko', '').split('|')
        if not toko_param or not toko_param[0]:
            return JsonResponse('', safe=False)

        query = """
            SELECT us.fn_userid as userid, emp.fv_nameemployees as employee_name FROM t_aksesusercust aksesuser
            JOIN customer_tb cus ON aksesuser.fn_cusid = cus.fn_cusid
            JOIN user_tb us ON aksesuser.fn_userid = us.fn_userid AND us.fn_userlevel = 1 AND us.fc_hold = 'F'
            JOIN employees_tb emp ON us.fn_userid = emp.fn_userid AND emp.fc_hold = 'F'
            WHERE us.fn_userid != %s AND cus.fn_cusid = %s ORDER BY emp.fv_nameemployees ASC
        """
        spg_list = DatabaseConnector.execute_raw_sql(query=query, params=[user_id, toko_param[0]], using='local')
        
        return JsonResponse(spg_list, safe=False)
    
    def get_absensi_by_level(self,request_data,decoded_payload, user_id):
        # Ambil userid dari session
        # user_id = session_data.get('newUser', {}).get('userid') if 'newUser' in session_data else session_data.get('userid')
        
        raw_level = request_data.GET.get('level',decoded_payload.get('userlevel'))
        tempat = request_data.GET.get('tempat','all')
        level = int(raw_level) if str(raw_level).isdigit() else raw_level
        # 3. Logika penentuan role menjadi sangat simple
        if level in (1,13,8,2, 12):
            level = 'operator' 
        # else :'manager'
        # print(level)
        db_results = []
            
        # === CASE SUPERVISOR ===
        if level == 'supervisor':
            periode = request_data.GET.get('periode')
            
            # Kondisi tempat dinamis
            tempat_clause = ""
            query_params = [periode, periode, user_id] # Urutan parameter pertama
            if tempat != 'all':
                tempat_clause = "and fn_supervisorid = %s"
                query_params.append(tempat) # Jika bukan 'all', tambah parameter tempat
            
            query = f"""
                select
                    ab.fd_tanggal,
                    e.fv_nameemployees,
                    ab.fc_jenis,
                    ab.fc_foto->>'$.in' as fc_foto_in,
                    ab.fc_foto->>'$.out' as fc_foto_out,
                    ab.fc_tempat->>'$.in' as fc_tempat_in,
                    ab.fc_tempat->>'$.out' as fc_tempat_out,
                    ab.fd_create,
                    ab.fd_update
                from (
                    select fd_tanggal, fn_userid, fc_jenis, fc_status, fc_foto, fc_tempat, fd_create, fd_update 
                    from absensi_dps_copy
                    where month(fd_tanggal) = month(%s)
                        and year(fd_tanggal) = year(%s)
                        and stat <> 'd'
                ) ab
                join (
                    select * from t_aksesusermanager where fn_managerid = %s {tempat_clause}
                ) u
                    on ab.fn_userid = u.fn_supervisorid
                join (
                    select fn_userid, fv_nameemployees from employees_tb
                    where fn_iddepart in (8,20)
                        and fc_hold = 'F'
                ) e
                    on u.fn_supervisorid = e.fn_userid
                group by ab.fd_tanggal, u.fn_supervisorid
                order by ab.fd_tanggal desc, e.fv_nameemployees
            """
            db_results = DatabaseConnector.execute_raw_sql(query=query, params=query_params, using='payroll')
    
            # # Kirim query bersama dengan list parameter kerjanya
            # cursor.execute(query, query_params)
            # db_results = DataModelHelper.dict_fetchall(cursor)
        
        # === CASE OPERATOR ===
        elif level == 'operator':
            periode_awal = request_data.GET.get('periodeAwal',datetime.today().strftime('%Y-%m-%d'))
            periode_akhir = request_data.GET.get('periodeAkhir',datetime.today().strftime('%Y-%m-%d'))
            raw_level = request_data.GET.get('level',decoded_payload.get('userlevel'))
            # Kondisi tempat dinamis
            if tempat != 'all':
                tempat_clause = "where fn_cusid = %s"
                tempat_clause2= "and c_in.fn_cusid = %s"
                # Urutan: periode_awal, periode_akhir, tempat, user_id
                query_params = [periode_awal, periode_akhir, tempat, user_id,tempat]
            else:
                tempat_clause = ""
                tempat_clause2= ""
                # Urutan: periode_awal, periode_akhir, user_id
                query_params = [periode_awal, periode_akhir, user_id]
            
            query = f"""
                select
                    ab.fd_tanggal,
                    e.fn_userid,
                    e.fv_nameemployees,
                    ab.fc_jenis,
                    ab.fc_shift->>'$.in' as fc_shift,
                    ab.fc_foto->>'$.in' as fc_foto_in,
                    ab.fc_foto->>'$.out' as fc_foto_out,
                    
                    fc_posisi_in_lati,
                    fc_posisi_in_long,
                    fc_posisi_out_lati,
                    fc_posisi_out_long,
                    c_in.fn_cusid cus_in,
                    c_out.fn_cusid cus_out,
                    c_in.fc_codecus cus_code_in,
                    c_out.fc_codecus cus_code_out,
                    e.fn_cus_stay fn_cusid,
                    -- c.fc_codecus,
                    -- c.fv_namecus,
                    ab.fc_jam->>'$.in' as fc_jm_in,
                    ab.fc_jam->>'$.out' as fc_jm_out,

                    SUBSTRING_INDEX(k_in.fv_coordinate, ' ', 1) AS fc_posisi_target_lati_in,
                    SUBSTRING_INDEX(k_in.fv_coordinate, ' ', -1) AS fc_posisi_target_long_in,
                    SUBSTRING_INDEX(k_out.fv_coordinate, ' ', 1) AS fc_posisi_target_lati_out,
                    SUBSTRING_INDEX(k_out.fv_coordinate, ' ', -1) AS fc_posisi_target_long_out
                from (
                    select fd_tanggal, fn_userid, fc_jenis, fc_status, fc_shift, fc_foto, fn_toko_in, 
                    fd_create, fd_update,fc_jam,
                    SUBSTRING_INDEX(fc_tempat->>'$.in', ' ', 1) AS fc_tempat_in,
                    SUBSTRING_INDEX(fc_tempat->>'$.out', ' ', 1) AS fc_tempat_out,
                    SUBSTRING_INDEX(fc_posisi->>'$.in', ' ', 1) AS fc_posisi_in_lati,
                    SUBSTRING_INDEX(fc_posisi->>'$.in', ' ', -1) AS fc_posisi_in_long,
                    SUBSTRING_INDEX(fc_posisi->>'$.out', ' ', 1) AS fc_posisi_out_lati,
                    SUBSTRING_INDEX(fc_posisi->>'$.out', ' ', -1) AS fc_posisi_out_long
                    from absensi_dps_copy
                    where fd_tanggal >= %s and fd_tanggal <= %s
                        and stat <> 'd'
                ) ab
                join (
                    select fn_userid, fc_hold from user_tb
                    where fn_userlevel in (1,8,13)
                ) u
                    on ab.fn_userid = u.fn_userid
                join (
                    select fn_userid, fv_nameemployees, fn_cus_stay, fc_hold from employees_tb
                    where fn_iddepart in (8,20)
                        and fc_hold = 'F'
                ) e
                    on u.fn_userid = e.fn_userid
                join (
                    select fn_userid, fn_cusid from t_aksesusercust
                    {tempat_clause}
                ) t
                    on u.fn_userid = t.fn_userid
                join (
                    select fn_cusid, fc_codecus, fv_namecus from customer_tb
                    where fc_hold = 'F'
                ) c
                    on t.fn_cusid = c.fn_cusid
                join (
                    select fn_userid, fn_cusid from t_aksesusercust
                    where fn_userid = %s
                ) ts
                    on c.fn_cusid = ts.fn_cusid
                
				LEFT JOIN customer_tb c_in  
					ON CONVERT(ab.fc_tempat_in USING latin1) = c_in.fc_codecus  
					AND c_in.fc_hold = 'F' 
				LEFT JOIN customer_tb c_out  
					ON CONVERT(ab.fc_tempat_out USING latin1) = c_out.fc_codecus  
					AND c_out.fc_hold = 'F' 
                LEFT JOIN m_koordinat_toko k_in 
					ON CONVERT(ab.fc_tempat_in USING latin1) = k_in.fc_codecus  
					-- AND k_in.fc_hold = 'F'  
                LEFT JOIN m_koordinat_toko k_out  
					ON CONVERT(ab.fc_tempat_out USING latin1) = k_out.fc_codecus  
					-- AND k_out.fc_hold = 'F'  
                where u.fc_hold = "F" and e.fc_hold = "F" {tempat_clause2}
                group by ab.fd_tanggal, u.fn_userid
                order by ab.fd_tanggal desc, e.fv_nameemployees
            """
            db_results = DatabaseConnector.execute_raw_sql(query=query, params=query_params, using='payroll',p_queri=False)
            df_toko_by_user=pd.DataFrame(db_results)

            def process_radius_in_out(row):
                # --- 1. Hitung Radius IN ---
                try:
                    # Lakukan cleaning koma seperti sebelumnya jika dibutuhkan (.split(',')[...])
                    abs_in = (
                        float(str(row["fc_posisi_in_lati"]).split(",")[0]),
                        float(str(row["fc_posisi_in_long"]).split(",")[-1]),
                    )
                    target_in = (
                        float(str(row["fc_posisi_target_lati_in"]).split(",")[0]),
                        float(str(row["fc_posisi_target_long_in"]).split(",")[-1]),
                    )
                    jarak_in = math.floor(geodesic(abs_in, target_in).meters)
                except (ValueError, TypeError, IndexError):
                    jarak_in = None

                # --- 2. Hitung Radius OUT ---
                try:
                    abs_out = (
                        float(str(row["fc_posisi_out_lati"]).split(",")[0]),
                        float(str(row["fc_posisi_out_long"]).split(",")[-1]),
                    )
                    target_out = (
                        float(str(row["fc_posisi_target_lati_out"]).split(",")[0]),
                        float(str(row["fc_posisi_target_long_out"]).split(",")[-1]),
                    )
                    jarak_out = math.floor(geodesic(abs_out, target_out).meters)
                except (ValueError, TypeError, IndexError):
                    jarak_out = None

                # Mengembalikan 2 nilai sekaligus untuk kolom baru
                return pd.Series([jarak_in, jarak_out])


            # Terapkan fungsi ke dataframe untuk membuat 2 kolom baru sekaligus
            df_toko_by_user[["jarak_radius_in", "jarak_radius_out"]] = df_toko_by_user.apply(
                process_radius_in_out, axis=1
            )

            # --- 3. RAPKAN DATA UNTUK JSON OUTPUT ---
            # Ubah tipe data menjadi Int64 agar aman mengurusi data bernilai None/NaN
            df_toko_by_user["jarak_radius_in"] = pd.to_numeric(
                df_toko_by_user["jarak_radius_in"], errors="coerce"
            ).astype("Int64")
            df_toko_by_user["jarak_radius_out"] = pd.to_numeric(
                df_toko_by_user["jarak_radius_out"], errors="coerce"
            ).astype("Int64")

            # Ganti NaN ke None agar JsonResponse Django tidak menghasilkan error 'NaN'
            df_toko_by_user = df_toko_by_user.where(pd.notnull(df_toko_by_user), None)
            
            list_toko=list_to_sql_in(df_toko_by_user,'fn_cusid')
            query_roster = f"""
                SELECT a.fn_userid, a.fn_cusid, fv_nameemployees,
                -- ifnull(fv_shift_name,fc_status) as fv_shift_name,fc_status,
                fd_schedule_date,
                -- c.fn_shift_id, fv_week_type, 
                fd_start_time, fd_end_time
                
                FROM absensi_roster a
                join (
                    select fn_userid, fv_nameemployees, fn_cus_stay,fc_hold from employees_tb
                    where fn_iddepart in (8,20)
                        and fc_hold = 'F'
                ) e
                    on a.fn_userid = e.fn_userid
                join (
                    select fn_userid,fc_hold from user_tb
                    where fn_userlevel in (1,8,13) and fc_hold = 'F'
                ) u
                    on a.fn_userid = u.fn_userid
                left join customer_shifts b on a.fn_shift_version_id=b.fn_shift_version_id
                left join shifts_tb c on a.fn_shift_id=c.fn_shift_id or b.fn_shift_id=c.fn_shift_id
                left join shift_segments d on a.fn_shift_id=d.fn_shift_id or b.fn_shift_id=d.fn_shift_id
                WHERE a.fn_cusid IN ({list_toko}) and u.fc_hold = "F" and e.fc_hold = "F" 
                and a.fd_schedule_date >= %s and a.fd_schedule_date <= %s
            """
            query_params = [periode_awal, periode_akhir]
            raw_data_1 = DatabaseConnector.execute_raw_sql(
                query=query_roster,
                params=query_params,
                using='payroll',
                p_queri=False
            )
            df_roster = pd.DataFrame(raw_data_1).fillna(0)
            if df_roster.empty:
                df_roster=pd.DataFrame(columns=['fn_userid','fn_cusid', 'fv_shift_name','fn_shift_version_id',
                                            'fv_shift_code', 'fd_start_time', 'fd_end_time',
                                            'fv_nameemployees','fn_shift_id','fv_week_type'])
                df_roster['fn_cusid']=df_roster['fn_cusid'].astype('int')
            df_toko_by_user=df_toko_by_user.merge(df_roster,on=['fn_userid','fn_cusid','fv_nameemployees'],how='outer')
            fill_values = {
                'object': '-',      # Untuk teks/kategori
                'int64': 0,               # Untuk angka bulat
                'float64': 0.0, # Untuk angka desimal (misal pakai median)
                'bool': False             # Untuk boolean
            }

            # 2. Buat mapping otomatis: {nama_kolom: nilai_pengganti}
            column_fill = {col: fill_values[str(dt)] for col, dt in df_toko_by_user.dtypes.items() if str(dt) in fill_values}
            df_toko_by_user = df_toko_by_user.fillna(value=column_fill)
            def tentukan_keterangan(row):
                # Ambil waktu sekarang untuk perbandingan jam pulang
                # (Di sini disesuaikan dengan format jam 'HH:MM:SS')
                now_time_str = datetime.now().strftime("%H:%M:%S")
                
                # 1. Cek status Shift Off / Sakit / Izin dsb
                shift = str(row['fc_shift']).lower().strip()
                if shift in ['off', 'sakit', 'izin', 'cuti']:
                    return "Off"
                    
                # 2. Cek apakah tidak ada jadwal shift
                # Jika start_time atau end_time berisi '-' atau kosong
                start_time = str(row['fd_start_time']).strip()
                end_time = str(row['fd_end_time']).strip()
                
                if start_time in ['-', ''] or end_time in ['-', '']:
                    return "Tidak ada jadwal shift"
                    
                # 3. Cek Belum Absen Masuk
                # Logika: fd_start_time terisi tapi fc_jm_in kosong atau '-'
                jam_in = str(row['fc_jm_in']).strip()
                if (start_time not in ['-', '']) and (jam_in in ['-', '']):
                    return "Belum absen masuk"
                    
                # 4. Cek Belum Absen Pulang
                # Logika: Sudah masuk (jam_in ada), tapi jam_out kosong DAN waktu sekarang melewati fd_end_time
                jam_out = str(row['fc_jm_out']).strip()
                if (jam_in not in ['-', '']) and (jam_out in ['-', '']):
                    if now_time_str > end_time:
                        return "Belum absen pulang"
                    else:
                        return "Sedang bekerja" # Tambahan opsional jika belum waktunya pulang
                        
                # 5. Jika sudah absen masuk dan pulang dengan normal
                if jam_in not in ['-', ''] and jam_out not in ['-', '']:
                    return "Hadir"
                    
                return "Data tidak valid"
            
            list_toko=list_to_sql_in(df_toko_by_user,'cus_in')
            df_cus = df__customers_pb_('fn_cusid cus_in,concat(fc_code," - ",fv_toko) fv_namecus',where=f'and fn_cusid IN ({list_toko})')
            df_toko_by_user = pd.merge(df_cus,df_toko_by_user,left_on='cus_in',right_on='cus_in',how='right')
            df_toko_by_user = df_toko_by_user.astype(object).fillna('-')
            # Terapkan fungsi ke DataFrame
            df_toko_by_user['keterangan_absen'] = df_toko_by_user.apply(tentukan_keterangan, axis=1)
            # 1. Hitung total per status (menggunakan value_counts agar semua status terhitung)
            summary_series = df_toko_by_user['keterangan_absen'].value_counts()

            # 2. Definisikan semua status yang mungkin muncul agar frontend selalu menerima key yang konsisten 
            # (Mencegah key hilang jika hari itu tidak ada yang 'Belum absen pulang', dll)
            all_statuses = [
                "Hadir", 
                "Belum absen masuk", 
                "Belum absen pulang", 
                "Sedang bekerja", 
                "Off", 
                "Tidak ada jadwal shift"
            ]

            # 3. Buat dictionary summary dengan nilai default 0, lalu update dengan hasil real data
            summary_dict = {status: 0 for status in all_statuses}
            for status, count in summary_series.items():
                if status in summary_dict:
                    summary_dict[status] = int(count) # diconvert ke int standard Python agar aman saat di-json.dumps

            # 4. Tambahkan total seluruh karyawan yang diproses hari itu
            summary_dict["total_karyawan"] = int(len(df_toko_by_user))
            list_user= df_toko_by_user['fn_userid'].drop_duplicates().tolist()
            # df_toko_by_user=df_toko_by_user.loc[df_toko_by_user['fn_userid']==145]
            return JsonResponse({
                                'result':df_toko_by_user.sort_values(by=['fv_namecus','fv_nameemployees']).to_dict(orient='records'),
                                "raw":list_user,
                                'sum_abs':summary_dict}
                                , safe=False)
        
        return JsonResponse({
                                    'result':{},
                                    "raw":{},
                                    'sum_abs':{}}
                                    , safe=False)
####################
    def get_absensi_by_level2(self, request_data, decoded_payload, user_id):
            raw_level = request_data.GET.get('level', decoded_payload.get('userlevel'))
            tempat = request_data.GET.get('tempat', 'all')
            level = int(raw_level) if str(raw_level).isdigit() else raw_level
            
            # Logika penentuan role
            if level in (1, 13, 8, 2, 12):
                level = 'operator' 
                
            db_results = []
                
            # === CASE SUPERVISOR ===
            if level == 'supervisor':
                # ... (Logika supervisor tetap sama atau direfactor dengan pola serupa jika dibutuhkan) ...
                pass

            # === CASE OPERATOR ===
            elif level == 'operator':
                periode_awal = request_data.GET.get('periodeAwal', datetime.today().strftime('%Y-%m-%d'))
                periode_akhir = request_data.GET.get('periodeAkhir', datetime.today().strftime('%Y-%m-%d'))

                # ---------------------------------------------------------
                # STEP 1: SCOPE KARYAWAN (DB: LIVE) & MAPPING CUS_STAY (DB: PAYROLL)
                # ---------------------------------------------------------
                
                # 1A. Tarik data karyawan dan toko dari DB Live (TANPA fn_cus_stay)
                df_toko_handle = self.df_toko_handle_(
                    colom='''u.fn_userid AS userid,
                            e.fv_nameemployees AS fv_nameemployees,
                            c.fn_cusid AS fn_cusid''', # e.fn_cus_stay dihapus dari sini
                    ts__fn_userid__in=[user_id], 
                    df=True, queri=False
                )
                
                if tempat != 'all':
                    df_toko_handle = df_toko_handle[df_toko_handle['fn_cusid'] == int(tempat)]
                    
                if df_toko_handle.empty:
                    return self._empty_response()
                list_toko=list_to_sql_in(df_toko_handle,'fn_cusid')
                df_cus = df__customers_pb_('fn_cusid ,concat(fc_code," - ",fv_toko) fv_namecus',where=f'and fn_cusid IN ({list_toko})')
                df_toko_handle = pd.merge(df_toko_handle, df_cus, on=['fn_cusid'], how='inner')
                list_userspg = df_toko_handle['userid'].unique().tolist()
                format_strings_user = ','.join(['%s'] * len(list_userspg))

                # 1B. Tarik data 'fn_cus_stay' dari m_cus_stay di DB Payroll
                query_cus_stay = f"""
                    SELECT fn_userid AS userid, fn_cusid  
                    FROM m_cus_stay 
                    WHERE fn_userid IN ({format_strings_user})
                """
                raw_cus_stay = DatabaseConnector.execute_raw_sql(
                    query=query_cus_stay, 
                    params=list_userspg, 
                    using='payroll', 
                    p_queri=False
                )
                df_cus_stay = pd.DataFrame(raw_cus_stay)

                # 1C. Gabungkan (Merge) fn_cus_stay ke df_toko_handle
                if not df_cus_stay.empty:
                    # Karena 1 user mungkin punya 1 cus_stay, kita gunakan left join
                    df_toko_handle = pd.merge(df_toko_handle, df_cus_stay, on=['userid','fn_cusid'], how='inner')
                else:
                    # Jika tabel kosong/tidak ada data, buat kolom default agar kode di bawahnya tidak error
                    df_toko_handle['fn_cus_stay'] = None

                # ---------------------------------------------------------
                # STEP 2: DATA ABSENSI (DB: PAYROLL)
                # ---------------------------------------------------------
                query_absensi = f"""
                    SELECT 
                        fd_tanggal,
                        fn_userid AS userid,
                        fc_jenis,
                        fc_shift->>'$.in' AS fc_shift,
                        fc_foto->>'$.in' AS fc_foto_in,
                        fc_foto->>'$.out' AS fc_foto_out,
                        fc_jam->>'$.in' AS fc_jm_in,
                        fc_jam->>'$.out' AS fc_jm_out,
                        SUBSTRING_INDEX(fc_tempat->>'$.in', ' ', 1) AS fc_tempat_in,
                        SUBSTRING_INDEX(fc_tempat->>'$.out', ' ', 1) AS fc_tempat_out,
                        SUBSTRING_INDEX(fc_posisi->>'$.in', ' ', 1) AS fc_posisi_in_lati,
                        SUBSTRING_INDEX(fc_posisi->>'$.in', ' ', -1) AS fc_posisi_in_long,
                        SUBSTRING_INDEX(fc_posisi->>'$.out', ' ', 1) AS fc_posisi_out_lati,
                        SUBSTRING_INDEX(fc_posisi->>'$.out', ' ', -1) AS fc_posisi_out_long
                    FROM absensi_dps_copy
                    WHERE fd_tanggal >= %s AND fd_tanggal <= %s
                    AND stat <> 'd'
                    AND fn_userid IN ({format_strings_user})
                """
                params_absensi = [periode_awal, periode_akhir] + list_userspg
                df_absensi = pd.DataFrame(DatabaseConnector.execute_raw_sql(
                    query_absensi, params=params_absensi, using='payroll', p_queri=False
                ))

                # ---------------------------------------------------------
                # STEP 3: DATA JADWAL / ROSTER (DB: PAYROLL)
                # ---------------------------------------------------------
                # Dibuat bersih tanpa join tabel user_tb/employees_tb Live DB
                query_roster = f"""
                    SELECT 
                        a.fn_userid AS userid, 
                        a.fn_cusid AS roster_cusid,
                        a.fd_schedule_date AS fd_tanggal,
                        fd_start_time, 
                        fd_end_time,
                        c.fn_shift_id, 
                        b.fv_week_type
                    FROM absensi_roster a
                    LEFT JOIN customer_shifts b ON a.fn_shift_version_id = b.fn_shift_version_id
                    LEFT JOIN shifts_tb c ON (a.fn_shift_id = c.fn_shift_id OR b.fn_shift_id = c.fn_shift_id)
                    left join shift_segments d on a.fn_shift_id=d.fn_shift_id or b.fn_shift_id=d.fn_shift_id
                    WHERE a.fd_schedule_date >= %s AND a.fd_schedule_date <= %s
                    AND a.fn_userid IN ({format_strings_user})
                """
                params_roster = [periode_awal, periode_akhir] + list_userspg
                df_roster = pd.DataFrame(DatabaseConnector.execute_raw_sql(
                    query_roster, params=params_roster, using='payroll', p_queri=False
                ))

                # ---------------------------------------------------------
                # STEP 4: MASTER TOKO & KOORDINAT (DB: LIVE)
                # ---------------------------------------------------------
                tempat_codes = []
                if not df_absensi.empty:
                    tempat_codes = pd.concat([df_absensi['fc_tempat_in'], df_absensi['fc_tempat_out']]).dropna().unique().tolist()
                    
                if tempat_codes:
                    format_tempat = ','.join(['%s'] * len(tempat_codes))
                    # Ambil Nama Toko & ID
                    q_cust = f"SELECT fn_cusid, fc_codecus, fv_namecus FROM customer_tb WHERE fc_codecus IN ({format_tempat})"
                    df_customer = pd.DataFrame(DatabaseConnector.execute_raw_sql(q_cust, params=tempat_codes, using='local'))
                    
                    # Ambil Koordinat
                    q_koord = f"""
                        SELECT 
                            fc_codecus, 
                            SUBSTRING_INDEX(fv_coordinate, ' ', 1) AS fc_posisi_target_lati,
                            SUBSTRING_INDEX(fv_coordinate, ' ', -1) AS fc_posisi_target_long 
                        FROM m_koordinat_toko 
                        WHERE fc_codecus IN ({format_tempat})
                    """
                    df_koordinat = pd.DataFrame(DatabaseConnector.execute_raw_sql(q_koord, params=tempat_codes, using='payroll'))
                else:
                    df_customer = pd.DataFrame(columns=['fn_cusid', 'fc_codecus', 'fv_namecus'])
                    df_koordinat = pd.DataFrame(columns=['fc_codecus', 'fc_posisi_target_lati', 'fc_posisi_target_long'])

                # ---------------------------------------------------------
                # STEP 5: PANDAS MERGING & DATA WRANGLING
                # ---------------------------------------------------------
                # Format tanggal agar tipe datanya selaras saat di-merge
                if not df_roster.empty:
                    df_roster['fd_tanggal'] = pd.to_datetime(df_roster['fd_tanggal']).dt.strftime('%Y-%m-%d')
                if not df_absensi.empty:
                    df_absensi['fd_tanggal'] = pd.to_datetime(df_absensi['fd_tanggal']).dt.strftime('%Y-%m-%d')

                # 5a. Gabung Roster dan Absensi (Outer Join berdasar USER dan TANGGAL)
                if not df_roster.empty and not df_absensi.empty:
                    df_merge = pd.merge(df_roster, df_absensi, on=['userid', 'fd_tanggal'], how='outer')
                elif not df_absensi.empty:
                    df_merge = df_absensi.copy()
                elif not df_roster.empty:
                    df_merge = df_roster.copy()
                else:
                    return self._empty_response()

                # 5b. Masukkan data Master Karyawan (Inner / Left join)
                # print(df_merge.info())
                # print(df_toko_handle.info())
                # print(df_merge.loc[df_merge['userid']==145])
                df_final = pd.merge(df_merge, df_toko_handle, on='userid', how='left')
                # print(df_final.loc[df_final['userid']==145])
            
                # 5c. Masukkan data Koordinat & Detail Toko IN
                if not df_koordinat.empty:
                    df_final = pd.merge(df_final, df_koordinat, left_on='fc_tempat_in', right_on='fc_codecus', how='left')
                    df_final.rename(columns={
                        'fc_posisi_target_lati': 'fc_posisi_target_lati_in',
                        'fc_posisi_target_long': 'fc_posisi_target_long_in'
                    }, inplace=True)
                    df_final.drop(columns=['fc_codecus'], inplace=True, errors='ignore')

                # 5d. Masukkan data Koordinat & Detail Toko OUT
                if not df_koordinat.empty:
                    df_final = pd.merge(df_final, df_koordinat, left_on='fc_tempat_out', right_on='fc_codecus', how='left')
                    df_final.rename(columns={
                        'fc_posisi_target_lati': 'fc_posisi_target_lati_out',
                        'fc_posisi_target_long': 'fc_posisi_target_long_out'
                    }, inplace=True)
                    df_final.drop(columns=['fc_codecus'], inplace=True, errors='ignore')

                # ---------------------------------------------------------
                # STEP 6: KALKULASI RADIUS & KETERANGAN
                # ---------------------------------------------------------
                def process_radius_in_out(row):
                    try:
                        abs_in = (float(str(row.get("fc_posisi_in_lati")).split(",")[0]), 
                                float(str(row.get("fc_posisi_in_long")).split(",")[-1]))
                        tgt_in = (float(str(row.get("fc_posisi_target_lati_in")).split(",")[0]), 
                                float(str(row.get("fc_posisi_target_long_in")).split(",")[-1]))
                        jarak_in = math.floor(geodesic(abs_in, tgt_in).meters)
                    except (ValueError, TypeError, IndexError):
                        jarak_in = None

                    try:
                        abs_out = (float(str(row.get("fc_posisi_out_lati")).split(",")[0]), 
                                float(str(row.get("fc_posisi_out_long")).split(",")[-1]))
                        tgt_out = (float(str(row.get("fc_posisi_target_lati_out")).split(",")[0]), 
                                float(str(row.get("fc_posisi_target_long_out")).split(",")[-1]))
                        jarak_out = math.floor(geodesic(abs_out, tgt_out).meters)
                    except (ValueError, TypeError, IndexError):
                        jarak_out = None
                    return pd.Series([jarak_in, jarak_out])

                if not df_final.empty:
                    df_final[["jarak_radius_in", "jarak_radius_out"]] = df_final.apply(process_radius_in_out, axis=1)
                    df_final["jarak_radius_in"] = pd.to_numeric(df_final["jarak_radius_in"], errors="coerce").astype("Int64")
                    df_final["jarak_radius_out"] = pd.to_numeric(df_final["jarak_radius_out"], errors="coerce").astype("Int64")

                # Handling Null sebelum validasi string Keterangan
                fill_values = {'object': '-', 'int64': 0, 'float64': 0.0, 'bool': False}
                column_fill = {col: fill_values[str(dt)] for col, dt in df_final.dtypes.items() if str(dt) in fill_values}
                df_final.fillna(value=column_fill, inplace=True)
                df_final = df_final.where(pd.notnull(df_final), None)

                def tentukan_keterangan(row):
                    now_time_str = datetime.now().strftime("%H:%M:%S")
                    shift = str(row.get('fc_shift', '')).lower().strip()
                    
                    if shift in ['off', 'sakit', 'izin', 'cuti']:
                        return "Off"
                        
                    start_time = str(row.get('fd_start_time', '')).strip()
                    end_time = str(row.get('fd_end_time', '')).strip()
                    
                    if start_time in ['-', ''] or end_time in ['-', '']:
                        return "Tidak ada jadwal shift"
                        
                    jam_in = str(row.get('fc_jm_in', '')).strip()
                    jam_out = str(row.get('fc_jm_out', '')).strip()
                    
                    if (start_time not in ['-', '']) and (jam_in in ['-', '']):
                        return "Belum absen masuk"
                        
                    if (jam_in not in ['-', '']) and (jam_out in ['-', '']):
                        if now_time_str > end_time:
                            return "Belum absen pulang"
                        else:
                            return "Sedang bekerja"
                            
                    if jam_in not in ['-', ''] and jam_out not in ['-', '']:
                        return "Hadir"
                        
                    return "Data tidak valid"

                df_final['keterangan_absen'] = df_final.apply(tentukan_keterangan, axis=1)

                # ---------------------------------------------------------
                # STEP 7: SUMMARY & RESPONSE
                # ---------------------------------------------------------
                all_statuses = [
                    "Hadir", "Belum absen masuk", "Belum absen pulang", 
                    "Sedang bekerja", "Off", "Tidak ada jadwal shift", "Data tidak valid"
                ]
                summary_dict = {status: 0 for status in all_statuses}
                summary_series = df_final['keterangan_absen'].value_counts()
                
                for status, count in summary_series.items():
                    if status in summary_dict:
                        summary_dict[status] = int(count)
                        
                summary_dict["total_karyawan"] = int(len(df_final))
                
                # Formatting Response
                df_final.rename(columns={'userid': 'fn_userid'}, inplace=True)
                df_final.sort_values(by=['fv_namecus', 'fv_nameemployees'], inplace=True)
                
                list_user= df_final['fn_userid'].drop_duplicates().tolist()
                # df_final=df_final.loc[df_final['fn_userid']==145]
                return JsonResponse({
                    'result': df_final.to_dict(orient='records'),
                    "raw": list_user, # Kosongkan atau isikan raw log sesuai kebutuhan
                    'sum_abs': summary_dict
                }, safe=False)

            return self._empty_response()

    # Helper Method (Tambahkan di class Anda agar return JSON kosong konsisten)
    def _empty_response(self):
        return JsonResponse({'result': [], 'raw': [], 'sum_abs': {}}, safe=False)
    
    def get_sum_absensi_by_user(self,request,decoded_payload):
        # Ambil userid dari session
        # user_id = session_data.get('newUser', {}).get('userid') if 'newUser' in session_data else session_data.get('userid')
        
        user_id = request.GET.get('userid',decoded_payload.get('userid'))
        level = request.GET.get('userlevel',decoded_payload.get('userlevel'))
        tgl_now=datetime.today().strftime('%Y-%m-%d')
        # tempat = request_data.GET.get('tempat')
        db_results = []
            
        # === CASE SUPERVISOR ===
        if int(level) in (1,2,12):
            # periode = request_data.GET.get('periode')
            
            # Kondisi tempat dinamis
            # tempat_clause = ""
            # query_params = [periode, periode, user_id] # Urutan parameter pertama
            
            # if tempat != 'all':
            #     tempat_clause = "and fn_supervisorid = %s"
            #     query_params.append(tempat) # Jika bukan 'all', tambah parameter tempat
            
            query = """
                select
                    u.fn_userid,
                    ab.fc_shift->>'$.in' as fc_shift,
                    c_in.fn_cusid cus_in,
                    c_out.fn_cusid cus_out,
                    ab.fc_jam->>'$.in' as fc_jm_in,
                    ab.fc_jam->>'$.out' as fc_jm_out
                from (
                    select fd_tanggal, fn_userid, fc_jenis, fc_status, fc_shift, fc_jam, fn_toko_in, fd_create, fd_update,
                    SUBSTRING_INDEX(fc_tempat->>'$.in', ' ', 1) AS fc_tempat_in,
                    SUBSTRING_INDEX(fc_tempat->>'$.out', ' ', 1) AS fc_tempat_out
                    from absensi_dps_copy
                    where fd_tanggal >= %s and fd_tanggal <= %s
                        and stat <> 'd'
                ) ab
                join (
                    select fn_userid from user_tb
                    where fn_userlevel in (1,8,13)
                ) u
                    on ab.fn_userid = u.fn_userid
                join (
                    select fn_userid, fv_nameemployees from employees_tb
                    where fn_iddepart in (8,20)
                        and fc_hold = 'F'
                ) e
                    on u.fn_userid = e.fn_userid
                join (
                    select fn_userid, fn_cusid from t_aksesusercust
                ) t
                    on u.fn_userid = t.fn_userid
                join (
                    select fn_cusid, fc_codecus, fv_namecus from customer_tb
                    where fc_hold = 'F'
                ) c
                    on t.fn_cusid = c.fn_cusid
                join (
                    select fn_userid, fn_cusid from t_aksesusercust
                    where fn_userid = %s
                ) ts
                    on c.fn_cusid = ts.fn_cusid
				LEFT JOIN customer_tb c_in  
					ON CONVERT(ab.fc_tempat_in USING latin1) = c_in.fc_codecus  
					AND c_in.fc_hold = 'F' 
				LEFT JOIN customer_tb c_out  
					ON CONVERT(ab.fc_tempat_out USING latin1) = c_out.fc_codecus  
					AND c_out.fc_hold = 'F' 
				GROUP BY 
					ab.fd_tanggal, 
					u.fn_userid,
					e.fv_nameemployees,
					ab.fc_shift->>'$.in',
					c_in.fc_codecus,
					c_out.fc_codecus,
					ab.fc_jam->>'$.in',
					ab.fc_jam->>'$.out'
				ORDER BY 
					ab.fd_tanggal DESC, 
					e.fv_nameemployees ASC
            """
            db_results = DatabaseConnector.execute_raw_sql(
                query=query,  params=[tgl_now,tgl_now,user_id], using='payroll',p_queri=False)
            
            df_absensi=pd.DataFrame(db_results)
            query_handle_toko = """
                    SELECT DISTINCT u.fn_userid,
                    e.fv_nameemployees,u.fn_userlevel, e.fn_cus_stay fn_cusid
                    FROM user_tb u
                    JOIN employees_tb e ON u.fn_userid = e.fn_userid
                    JOIN t_aksesusercust t ON u.fn_userid = t.fn_userid
                    JOIN customer_tb c ON t.fn_cusid = c.fn_cusid
                    JOIN t_aksesusercust ts ON c.fn_cusid = ts.fn_cusid
                    WHERE u.fn_userlevel IN (1, 8, 13) AND e.fn_iddepart IN (8, 20)
                    AND e.fc_hold = 'F' AND u.fc_hold = 'F' AND c.fc_hold = 'F' AND ts.fn_userid = %s
                    ORDER BY e.fn_cus_stay;
                """
            raw_data = DatabaseConnector.execute_raw_sql(
                    query=query_handle_toko, params=[user_id], using='payroll'
                    )
            df_toko_by_user=pd.DataFrame(raw_data)
            list_toko=list_to_sql_in(df_toko_by_user,'fn_cusid')
            query_roster = f"""
                SELECT fn_userid, a.fn_cusid, 
                -- ifnull(fv_shift_name,fc_status) as fv_shift_name,fc_status,
                fd_schedule_date,
                -- c.fn_shift_id, fv_week_type, 
                fd_start_time, fd_end_time
                FROM absensi_roster a
                left join customer_shifts b on a.fn_shift_version_id=b.fn_shift_version_id
                left join shifts_tb c on a.fn_shift_id=c.fn_shift_id or b.fn_shift_id=c.fn_shift_id
                left join shift_segments d on a.fn_shift_id=d.fn_shift_id or b.fn_shift_id=d.fn_shift_id
                WHERE a.fn_cusid IN ({list_toko}) and a.fd_schedule_date = %s

            """
            query_params = [tgl_now]
            raw_data_1 = DatabaseConnector.execute_raw_sql(
                query=query_roster,
                params=query_params,
                using='payroll',
                p_queri=False
            )
            df_roster = pd.DataFrame(raw_data_1).fillna(0)
            if df_roster.empty:
                df_roster=pd.DataFrame(columns=['fn_userid','fn_cusid', 'fv_shift_name','fn_shift_version_id',
                                            'fv_shift_code', 'fd_start_time', 'fd_end_time',
                                            'fn_shift_id','fv_week_type'])
                
            df_absens_sum=df_toko_by_user.merge(df_absensi,on = 'fn_userid',how='left')
            df_absens_sum_left=df_absens_sum.merge(df_roster,on = ['fn_userid','fn_cusid'],how='left').fillna('-')
            df_absens_sum=df_absens_sum.merge(df_roster,on = ['fn_userid','fn_cusid'],how='inner').fillna('-')


            ####
            time_str = request.GET.get('jam',datetime.now().strftime('%H:%M:%S'))
            # Catatan: Jika ingin testing manual dengan jam tertentu, ganti baris atas dengan:
            time_str = "18:00:00" 
            time_str = "23:00:00" 
            # Ubah string waktu sekarang menjadi objek timedelta agar bisa dihitung/dibandingkan
            ####
           
            time_now = pd.to_timedelta(time_str)
            # 3. Konversi kolom jam di df menjadi tipe timedelta
            df_absens_sum['jam_start'] = pd.to_timedelta(df_absens_sum['fd_start_time'].astype(str))
            df_absens_sum['jam_end'] = pd.to_timedelta(df_absens_sum['fd_end_time'].astype(str))

            # fd_start_time - 1 jam
            df_absens_sum['batas_datang'] = df_absens_sum['jam_start'] - pd.to_timedelta('1h')
            df_absens_sum['fc_jm_in'] = df_absens_sum['fc_jm_in'].astype(str).str.strip()
            df_absens_sum['fc_jm_out'] = df_absens_sum['fc_jm_out'].astype(str).str.strip()
            # 4. Buat Kondisi / Filter
            # Kondisi Belum Absen Datang: waktu sekarang > (start - 1 jam) DAN fc_jm_in kosong/null
            kondisi_belum_datang = (
            (time_now > df_absens_sum['batas_datang']) & 
            (df_absens_sum['fc_jm_in'].isna() | (df_absens_sum['fc_jm_in'] == '') | (df_absens_sum['fc_jm_in'] == 'None') | (df_absens_sum['fc_jm_in'] == 'nan'))
            )

            # Kondisi Pulang
            kondisi_belum_pulang = (
                (time_now > df_absens_sum['jam_end']) & 
                (df_absens_sum['fc_jm_out'].isna() | (df_absens_sum['fc_jm_out'] == '') | (df_absens_sum['fc_jm_out'] == 'None') | (df_absens_sum['fc_jm_out'] == 'nan'))
            )

            # 5. Berikan flag (1 jika memenuhi kondisi, 0 jika tidak)
            df_absens_sum['belum_absen_datang'] = kondisi_belum_datang.astype(int)
            df_absens_sum['belum_absen_pulang'] = kondisi_belum_pulang.astype(int)

            # 6. Groupby berdasarkan Toko (fn_cusid) untuk menjumlahkan totalnya
            df_summary = df_absens_sum.groupby('fn_cusid').agg(
                total_belum_absen_datang=('belum_absen_datang', 'sum'),
                total_belum_absen_pulang=('belum_absen_pulang', 'sum')
            ).reset_index()
            
            list_toko=list_to_sql_in(df_summary,'fn_cusid')
            df_cus=df__customers_pb_("fn_cusid,fv_toko 'Cus Name',fc_code Code",where=f'and fn_cusid in({list_toko})')
            
            if not df_summary.empty:
                list_toko = list_to_sql_in(df_summary, 'fn_cusid')
                df_cus = df__customers_pb_("fn_cusid,fv_toko 'Cus Name',fc_code Code", where=f'and fn_cusid in({list_toko})')
                df_summary = df_cus.merge(df_summary, on='fn_cusid', how='right')
            else:
                # Jika kosong, buat DataFrame dengan struktur kolom yang sesuai agar pemanggil tidak KeyError
                df_summary = pd.DataFrame(columns=['fn_cusid', 'Cus Name', 'Code', 'total_belum_absen_datang', 'total_belum_absen_pulang'])
            
            print(f"Waktu Sekarang Diproses: {time_now}")
        
        # === CASE OPERATOR ===
        elif level in (4,13):
            # periode_awal = request_data.GET.get('periodeAwal')
            # periode_akhir = request_data.GET.get('periodeAkhir')
            
            # # Kondisi tempat dinamis
            # if tempat != 'all':
            #     tempat_clause = "where fn_cusid = %s"
            #     # Urutan: periode_awal, periode_akhir, tempat, user_id
            #     query_params = [periode_awal, periode_akhir, tempat, user_id]
            # else:
            #     tempat_clause = ""
            #     # Urutan: periode_awal, periode_akhir, user_id
            #     query_params = [periode_awal, periode_akhir, user_id]
            
            query = f"""
                select
                    u.fn_userid,
                    e.fv_nameemployees,
                    ab.fc_shift->>'$.in' as fc_shift,
                    c_in.fn_cusid cus_in,
                    c_out.fn_cusid cus_out,
                    ab.fc_jam->>'$.in' as fc_jm_in,
                    ab.fc_jam->>'$.out' as fc_jm_out
                from (
                    select fd_tanggal, fn_userid, fc_jenis, fc_status, fc_shift, fc_jam, fn_toko_in, fd_create, fd_update,
                    SUBSTRING_INDEX(fc_tempat->>'$.in', ' ', 1) AS fc_tempat_in,
                    SUBSTRING_INDEX(fc_tempat->>'$.out', ' ', 1) AS fc_tempat_out
                    from absensi_dps_copy
                    where fd_tanggal >= "2026-06-29" and fd_tanggal <= "2026-06-29"
                        and stat <> 'd'
                ) ab
                join (
                    select fn_userid from user_tb
                    where fn_userlevel in (1,8,13)
                ) u
                    on ab.fn_userid = u.fn_userid
                join (
                    select fn_userid, fv_nameemployees from employees_tb
                    where fn_iddepart in (8,20)
                        and fc_hold = 'F'
                ) e
                    on u.fn_userid = e.fn_userid
                join (
                    select fn_userid, fn_cusid from t_aksesusercust
                ) t
                    on u.fn_userid = t.fn_userid
                join (
                    select fn_cusid, fc_codecus, fv_namecus from customer_tb
                    where fc_hold = 'F'
                ) c
                    on t.fn_cusid = c.fn_cusid
                join (
                    select fn_userid, fn_cusid from t_aksesusercust
                    where fn_userid = %s
                ) ts
                    on c.fn_cusid = ts.fn_cusid
				LEFT JOIN customer_tb c_in  
					ON CONVERT(ab.fc_tempat_in USING latin1) = c_in.fc_codecus  
					AND c_in.fc_hold = 'F' 
				LEFT JOIN customer_tb c_out  
					ON CONVERT(ab.fc_tempat_out USING latin1) = c_out.fc_codecus  
					AND c_out.fc_hold = 'F' 
				GROUP BY 
					ab.fd_tanggal, 
					u.fn_userid,
					e.fv_nameemployees,
					ab.fc_shift->>'$.in',
					c_in.fc_codecus,
					c_out.fc_codecus,
					ab.fc_jam->>'$.in',
					ab.fc_jam->>'$.out'
				ORDER BY 
					ab.fd_tanggal DESC, 
					e.fv_nameemployees ASC
            """
            db_results = DatabaseConnector.execute_raw_sql(query=query, params=query_params, using='payroll')
        result = {
            "sum_abs": df_summary.to_dict(orient='records'),
            "raw2": df_absens_sum.to_dict(orient='records'),
            "raw3": df_absens_sum_left.to_dict(orient='records'),
            # "sum_abs": db_results.sort_values('fc_code').to_dict(orient='records'),
        }
        
        return JsonResponse(result, safe=False)
    
    def get_store_user(self, request):
        # 1. Proteksi keamanan: pastikan user sudah login
        if not request.session.get('userid'):
            return Response(
                {'success': False, 'message': 'Unauthorized. Silakan login kembali.'}, 
                status=status.HTTP_401_UNAUTHORIZED
            )
            
        try:
            # 2. Panggil helper dengan parameter data request GET dan data session
            # Di Laravel: $store = UserModel::getStoreByUser($request);
            fake_session = {
                'newUser': {
                    'userid': 3,
                    'userlevel': 2  # Set ke 2 atau 12 jika ingin tes sebagai Supervisor
                }
            }
            store_data = UserModelHelper.get_store_by_user(request.GET, fake_session)
            
            # 3. Kembalikan hasilnya dalam bentuk Response JSON (seperti response()->json())
            return Response(store_data, status=status.HTTP_200_OK)
            
        except Exception as e:
            return Response(
                {'success': False, 'error': str(e)}, 
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
        
    def update_status(self,request,session_data,super_user,user_special):
        try:
            # Ambil data dari data session Django
            user_id= request.GET.get('userid', session_data.get('userid'))
            user_level = int(request.GET.get('userlevel',int(session_data.get('userlevel', 0))))
            full_name = request.GET.get('userlogin',session_data.get('userlogin', ''))
            stay_id = request.GET.get('stay',session_data.get('stay'))
            now = datetime.now()
            today_str = now.strftime('%Y-%m-%d')  # Format Y-m-d

            cek = None
            
            # Inisialisasi awal variabel data (Dictionary Python)
            data = {
                'userid': user_id,
                'userlevel': user_level,
                'username': full_name,
                'statusin': "",
                'statusout': "disabled",
                'lastno': 0,
                'lasttempat': "",
                'lastshift': "",
                'lasttanggal': "",
                'lastdate': ""
            }
            # ==========================================
            # CASE USERLEVEL == 0 (STAFF)
            # ==========================================
            if user_level == 0:
                query_staff = """
                    SELECT 
                        fd_tanggal AS lasttanggal,
                        fn_no AS lastno,
                        fc_status AS laststatus,
                        fc_shift AS lastshift,
                        fc_tempat AS lasttempat
                    FROM absensi_staff
                    WHERE fn_userid = %s AND DATE(fd_tanggal) <= %s
                    ORDER BY fd_tanggal DESC, fn_no DESC
                    LIMIT 1
                """
                cek = DatabaseConnector.execute_raw_sql(
                    query=query_staff, params=[user_id, today_str], using='payroll', fetch_one=True
                )
                if cek:
                    # Format date ke 'j/n/y' -> contoh: '10/6/26'
                    last_dt = datetime.strptime(str(cek['lasttanggal']), '%Y-%m-%d') if isinstance(cek['lasttanggal'], str) else cek['lasttanggal']
                    data['lasttanggal'] = f"{last_dt.day}/{last_dt.month}/{str(last_dt.year)[2:]}"
                    data['lastdate'] = str(cek['lasttanggal'])

                    # Kondisi tanggal kemarin / lalu
                    if str(cek['lasttanggal']) != today_str:
                        if cek['lasttempat']:
                            tempat_json = json.loads(cek['lasttempat']) if isinstance(cek['lasttempat'], str) else cek['lasttempat']
                            data['lasttempat'] = tempat_json.get('in', '')
                    # Kondisi tanggal hari ini
                    else:
                        data['statusin'] = 'disabled' if cek['laststatus'] == 'in' else ''
                        data['statusout'] = '' if cek['laststatus'] == 'in' else 'disabled'
                        data['lastno'] = cek['lastno'] if cek['laststatus'] == 'in' else 0
                        
                        if cek['lasttempat']:
                            tempat_json = json.loads(cek['lasttempat']) if isinstance(cek['lasttempat'], str) else cek['lasttempat']
                            data['lasttempat'] = tempat_json.get('in', '')
                        
                        if cek['lastshift'] and cek['laststatus'] == 'in':
                            shift_json = json.loads(cek['lastshift']) if isinstance(cek['lastshift'], str) else cek['lastshift']
                            data['lastshift'] = shift_json.get('in', '')
                        else:
                            data['lastshift'] = ''

            # ==========================================
            # CASE USERLEVEL != 0 (FIELD AGENT / SPG / SPV)
            # ==========================================
            else:
                
                data['statusoff'] = True
                
                query_dps = """
                    SELECT 
                        fd_tanggal AS lasttanggal,
                        fc_jenis AS lastjenis,
                        fn_no AS lastno,
                        fc_status AS laststatus,
                        fc_shift AS lastshift,
                        fc_tempat AS lasttempat
                    FROM absensi_dps
                    WHERE fn_userid = %s AND DATE(fd_tanggal) <= %s and stat <> 'd'
                    ORDER BY fd_tanggal DESC, fn_no DESC
                    LIMIT 1
                """
                cek = DatabaseConnector.execute_raw_sql(
                    query=query_dps, params=[user_id, today_str], using='payroll', fetch_one=True
                )

                if cek:
                    last_dt = datetime.strptime(str(cek['lasttanggal']), '%Y-%m-%d') if isinstance(cek['lasttanggal'], str) else cek['lasttanggal']
                    formatted_date = f"{last_dt.day}/{last_dt.month}/{str(last_dt.year)[2:]}"
                    data['lasttanggal'] = f"{formatted_date} ({str(cek['lastjenis']).upper()})"
                    data['lastdate'] = str(cek['lasttanggal'])

                    # Kondisi tanggal lalu
                    if str(cek['lasttanggal']) != today_str:
                        if cek['lastjenis'] == 'absen' and cek['lasttempat']:
                            tempat_json = json.loads(cek['lasttempat']) if isinstance(cek['lasttempat'], str) else cek['lasttempat']
                            data['lasttempat'] = tempat_json.get('in', '')
                    # Kondisi tanggal hari ini
                    else:
                        # Jika jenisnya BUKAN 'absen' (misal: Cuti/Izin/Sakit)
                        if cek['lastjenis'] != 'absen':
                            data['statusin'] = "disabled"
                            data['statusout'] = "disabled"
                            data['statusoff'] = False
                            
                            # Flash message disimpan ke session Django
                            session_data['result'] = f"Hari ini Anda {cek['lastjenis']}, akses absen ditutup!"
                            session_data['type'] = "warning"
                        else:
                            data['statusin'] = 'disabled' if cek['laststatus'] == 'in' else ''
                            data['statusout'] = '' if cek['laststatus'] == 'in' else 'disabled'
                            data['statusoff'] = False
                            data['lastno'] = cek['lastno'] if cek['laststatus'] == 'in' else 0
                            
                            if cek['lasttempat']:
                                tempat_json = json.loads(cek['lasttempat']) if isinstance(cek['lasttempat'], str) else cek['lasttempat']
                                data['lasttempat'] = tempat_json.get('in', '')
                            
                            if cek['lastshift'] and cek['laststatus'] == 'in':
                                shift_json = json.loads(cek['lastshift']) if isinstance(cek['lastshift'], str) else cek['lastshift']
                                data['lastshift'] = shift_json.get('in', '')
                            else:
                                data['lastshift'] = ''

            # ==========================================
            # LOAD MASTER DATA TOKO (CUSTOMER LIST)
            # ==========================================
            query_toko = """
                SELECT 
                    fn_cusid AS cusid,
                    fv_namecus AS cusname,
                    fc_codecus AS cuscode
                FROM customer_tb
                WHERE fc_hold = 'F'
                ORDER BY cusid ASC
            """
            data['storeList'] = DatabaseConnector.execute_raw_sql(query=query_toko, using='payroll')
            
            
            # ==========================================
            # HITUNG WAITING APPROVAL PENGAJUAN
            # ==========================================
            if user_level != 0:
                pengajuan_belum_approve = []
                
                # Level SPG / SPV Lapangan (1, 13, 8)
                if user_level in [1, 13, 8]:
                    query_app_staff = """
                        SELECT fc_jenis AS waiting_type, COUNT(fc_approve) AS waiting_qty
                        FROM approval_dps_dev
                        WHERE fn_userid = %s AND fc_approve = 'F'
                        GROUP BY fc_jenis ORDER BY fc_jenis
                    """
                    pengajuan_belum_approve = DatabaseConnector.execute_raw_sql(
                        query=query_app_staff, params=[user_id], using='payroll'
                    )
                
                # Level Supervisor / Alokator (2, 12)
                elif user_level in [2, 12]:
                    query_app_spv = """
                        SELECT a.fc_jenis AS waiting_type, COUNT(a.fc_approve) AS waiting_qty
                        FROM (
                            SELECT fn_userid, fc_approve, fc_jenis FROM approval_dps_dev
                            WHERE (MONTH(fd_mulai) = MONTH(NOW()) OR MONTH(fd_mulai) = MONTH(NOW()) - 1)
                              AND YEAR(fd_tanggal) = YEAR(NOW()) AND fc_approve = 'F'
                        ) a
                        INNER JOIN employees_tb e ON e.fn_userid = a.fn_userid
                        INNER JOIN user_tb u ON e.fn_userid = u.fn_userid AND u.fn_userlevel IN (1, 13)
                        INNER JOIN (SELECT fn_userid, fn_cusid FROM t_aksesusercust GROUP BY fn_userid) t ON t.fn_userid = a.fn_userid
                        INNER JOIN customer_tb c ON t.fn_cusid = c.fn_cusid AND c.fc_hold = 'F'
                        INNER JOIN t_aksesusercust ts ON c.fn_cusid = ts.fn_cusid AND ts.fn_userid = %s
                        GROUP BY a.fc_jenis ORDER BY a.fc_jenis
                    """
                    pengajuan_belum_approve = DatabaseConnector.execute_raw_sql(
                        query=query_app_spv, params=[user_id], using='payroll'
                    )

                data['pengajuan'] = pengajuan_belum_approve

                # Jika SPV (2, 12), ambil list toko yang dia handle secara spesifik
                if user_level in [2, 12]:
                    query_handle_toko = """
                        SELECT fn_cusid AS storeid, fc_codecus AS storecode, fv_namecus AS storename
                        FROM customer_tb
                        WHERE fn_cusid IN (SELECT ta.fn_cusid FROM t_aksesusercust ta WHERE ta.fn_userid = %s)
                        ORDER BY fn_grpcusid ASC, fc_codecus ASC
                    """
                    data['listTokoHandle'] = DatabaseConnector.execute_raw_sql(
                        query=query_handle_toko, params=[user_id], using='payroll'
                    )

                # Ambil Instruksi Lembur untuk Level (1, 13, 8)
                if user_level in [1, 13, 8]:
                    # Cari awal dan akhir bulan (Pengganti Carbon)
                    start_month = now.replace(day=1).strftime('%Y-%m-%d')
                    last_day = monthrange(now.year, now.month)[1]
                    end_month = now.replace(day=last_day).strftime('%Y-%m-%d')

                    query_lembur = """
                        SELECT * FROM approval_dps_dev
                        WHERE fn_userid = %s AND fc_jenis = 'lembur' AND fc_approve = 'Y'
                          AND DATE(fd_mulai) >= %s AND DATE(fd_mulai) <= %s
                        ORDER BY fd_mulai ASC
                    """
                    instruksi_lembur = DatabaseConnector.execute_raw_sql(
                        query=query_lembur, params=[user_id, start_month, end_month], using='payroll'
                    )

                    if instruksi_lembur:
                        data['instruksiLembur'] = {
                            'qty': len(instruksi_lembur),
                            'list': instruksi_lembur
                        }

            # ==========================================
            # PILIHAN TOKO AKTIF (STAY STORE)
            # ==========================================
            
            query_stay_store = """
                SELECT fc_codecus, fv_namecus, fn_cusid FROM customer_tb WHERE fn_cusid = %s LIMIT 1
            """
            store_user = DatabaseConnector.execute_raw_sql(
                query=query_stay_store, params=[stay_id], using='payroll', fetch_one=True
            )
            # Cek session 'store', jika tidak ada gabungkan code dan nama toko aktif
            
            if session_data.get('store'):
                
                data['store'] = session_data.get('store')
            else:
                data['store'] = f"{store_user['fc_codecus']} {store_user['fv_namecus']}" if store_user else ""
                data['storeid'] = (store_user.get('fn_cusid') if store_user else 0) or 0
            data['super_user'] = super_user
            
            query_shift = """
                select fv_shift_code, fv_shift_name, fd_start_time, fd_end_time from absensi_roster a 
                left join shift_segments ss on a.fn_shift_id=ss.fn_shift_id
                JOIN shifts_tb s ON s.fn_shift_id = ss.fn_shift_id  
                where fn_userid =%s and fd_schedule_date=%sLIMIT 1
            """
            raw_shift = DatabaseConnector.execute_raw_sql(
                query=query_shift, params=[user_id,today_str], using='payroll', fetch_one=True
            )
            if raw_shift:
                data['shift'] = {
                    'fv_shift_code': raw_shift['fv_shift_code'],
                    'fv_shift_name': raw_shift['fv_shift_name'],
                    'fd_start_time': raw_shift['fd_start_time'],
                    'fd_end_time': raw_shift['fd_end_time'],
                }
            else:
                data['shift'] = {
                    'fv_shift_code': '-',
                    'fv_shift_name': '-',
                    'fd_start_time': '00:00:00',
                    'fd_end_time': '00:00:00',
                }
            query_status_today = """
                SELECT 
                        fc_jenis AS today_jenis,
                        fc_status AS today_status
                    FROM gishrdpayroll.absensi_dps
                    WHERE fn_userid = %s AND DATE(fd_tanggal) = %s and stat <> 'd'
                    ORDER BY fd_tanggal DESC, fn_no DESC
                    LIMIT 1
            """
            raw_status_today = DatabaseConnector.execute_raw_sql(
                query=query_status_today, params=[user_id,today_str], using='payroll', fetch_one=True
            )
            if raw_status_today:
                jenis = raw_status_today['today_jenis']
                status = raw_status_today['today_status']
                
                # Jika jenis sama dengan status, maka 'off'
                if jenis == status:
                    data['status_today'] = 'off'
                # Jika tidak sama, maka jenis + ' ' + status
                else:
                    data['status_today'] = f"{jenis} {status}"

            else:
                # Jika raw_status_today kosong (tidak ada data absen hari ini)
                if data['shift']['fv_shift_name'] == '-':
                    data['status_today'] = 'no shift'
                else:
                    # Jika raw_status_today kosong tapi data['shift'] ada isinya
                    data['status_today'] = data['shift']['fv_shift_name']
            return JsonResponse({"result":data,
                                 "sesson":session_data}, safe=False)

        except Exception as e:
            return self._catch_error_details(e)
        
    def get_store_by_user(self,request, userid,level):
        """
        Mengambil data store berdasarkan level user dari session.
        Padanan dari static function getStoreByUser($data) di Laravel.
        
        Args:
            request: Objek HTTP request Django (untuk mengambil session)
            userid: ID user yang dicari datanya (padanan dari $data->user)
        """
        # 1. Ambil data userlevel dari session Django
        # session('newUser')['userlevel'] di Laravel -> request.session.get('newUser', {}).get('userlevel')
        # new_user_session = request.get('newUser', {})
        # level = new_user_session.get('userlevel')
        # 2. Kondisi untuk Level 2 dan 12 (Direct Access)
        if level in [2, 12]:
            query = """
                SELECT 
                    c.fn_cusid,
                    c.fc_codecus,
                    c.fv_namecus
                FROM customer_tb AS c
                INNER JOIN t_aksesusercust AS t ON t.fn_cusid = c.fn_cusid
                WHERE t.fn_userid = %s
            """
            params = [userid]
            
        # 3. Kondisi untuk Manager (Hierarchical Access via Supervisor)
        else:
            query = """
                SELECT 
                    c.fn_cusid,
                    c.fc_codecus,
                    c.fv_namecus
                FROM customer_tb AS c
                INNER JOIN t_aksesusercust AS t ON t.fn_cusid = c.fn_cusid
                INNER JOIN t_aksesusermanager AS tm ON t.fn_userid = tm.fn_supervisorid
                WHERE tm.fn_managerid = %s
                ORDER BY c.fc_codecus ASC
            """
            params = [userid]

        # 4. Eksekusi query menggunakan DatabaseConnector bawaan Anda
        store = DatabaseConnector.execute_raw_sql(query, params, using='payroll', return_dict=True)

        return JsonResponse(store, safe=False) 
    
    def get_data_pengajuan(self, request, decoded_payload):
        # 1. Ambil input request
        user_id = request.GET.get('userid', decoded_payload.get('userid'))
        
        # Ambil waktu saat ini untuk dijadikan default
        now = datetime.now()
        
        # Ambil parameter, jika tidak ada/kosong, otomatis gunakan bulan & tahun ini
        bulan_req = request.GET.get('bulan', str(now.month))  # Default: bulan ini (e.g., '7')
        tahun_req = request.GET.get('tahun', str(now.year))   # Default: tahun ini (e.g., '2026')
        
        # 2. Eksekusi Query
        query = "SELECT * FROM view_rekap_pengajuan WHERE userid = %s"
        params = [user_id]
        raw = DatabaseConnector.execute_raw_sql(query, params, using='payroll', return_dict=True)
            
        df = pd.DataFrame(raw)
        if df.empty:
            return JsonResponse({"draw": int(request.GET.get('draw', 1)), "recordsTotal": 0, "recordsFiltered": 0, "data": []})

        # 3. Filtering Menggunakan Pandas Datetime Vectorization
        df['mulai_dt'] = pd.to_datetime(df['mulai'])

        # Filter Tahun (Hanya dilewati jika frontend sengaja mengirim string 'all')
        if tahun_req != 'all':
            try:
                df = df[df['mulai_dt'].dt.year == int(tahun_req)]
            except ValueError:
                pass

        # Filter Bulan (Hanya dilewati jika frontend sengaja mengirim string 'all')
        if bulan_req != 'all':
            try:
                df = df[df['mulai_dt'].dt.month == int(bulan_req)]
            except ValueError:
                pass

        # Jika setelah difilter data bulan/tahun ini kosong
        if df.empty:
            return JsonResponse({"draw": int(request.GET.get('draw', 1)), "recordsTotal": 0, "recordsFiltered": 0, "data": []})

        # 4. Sorting Data
        df = df.sort_values(by=['no', 'mulai'], ascending=[False, False])

        # Mapping nama bulan Indonesia
        months = {
            1: 'Januari', 2: 'Februari', 3: 'Maret', 4: 'April', 
            5: 'Mei', 6: 'Juni', 7: 'Juli', 8: 'Agustus', 
            9: 'September', 10: 'Oktober', 11: 'November', 12: 'Desember'
        }

        # 5. Loop & Transform Data
        data_list = []
        df['sampai_dt'] = pd.to_datetime(df['sampai'])

        for _, row in df.iterrows():
            if row['status'] == 'Y':
                st = 'fa fa-thumbs-up text-success'
            elif row['status'] == 'N':
                st = 'fa fa-thumbs-down text-danger'
            else:
                st = ''

            mulai_dt = row['mulai_dt']
            sampai_dt = row['sampai_dt']
            
            bln_mulai = months.get(mulai_dt.month, '')
            bln_sampai = months.get(sampai_dt.month, '')

            durasi = ''
            jenis_pengajuan = str(row['jenis']).lower()

            string_tgl_mulai = f"{mulai_dt.strftime('%d')} {bln_mulai} {mulai_dt.strftime('%Y')}"
            string_tgl_sampai = f"{sampai_dt.strftime('%d')} {bln_sampai} {sampai_dt.strftime('%Y')}"

            if jenis_pengajuan == 'lembur':
                string_jam_mulai = mulai_dt.strftime('%H:%M')
                string_jam_sampai = sampai_dt.strftime('%H:%M')
                durasi = f"""
                <li class="list-group-item d-flex justify-content-between align-items-center p-1"><span>Tgl lembur:</span><span>{string_tgl_mulai}</span></li>
                <li class="list-group-item d-flex justify-content-between align-items-center p-1"><span>Waktu:</span><span>{string_jam_mulai} - {string_jam_sampai}</span></li>
                """
            elif jenis_pengajuan == 'tukar off':
                durasi = f"""
                <li class="list-group-item d-flex justify-content-between align-items-center p-1"><span>Tgl off awal:</span><span>{string_tgl_mulai}</span></li>
                <li class="list-group-item d-flex justify-content-between align-items-center p-1"><span>Tukar off tgl:</span><span>{string_tgl_sampai}</span></li>
                """
            else:
                durasi = f"""
                <li class="list-group-item d-flex justify-content-between align-items-center p-1"><span>Mulai tgl:</span><span>{string_tgl_mulai}</span></li>
                <li class="list-group-item d-flex justify-content-between align-items-center p-1"><span>Sampai tgl:</span><span>{string_tgl_sampai}</span></li>
                """

            status_map = {'F': ('menunggu', 'bg-secondary'), 'Y': ('disetujui', 'bg-success')}
            status_approval, warna_status = status_map.get(row['status'], ('ditolak', 'bg-danger'))

            catatan_atasan = str(row['note']).lower().capitalize() if row['note'] else ""

            ul = f"""<ul class="list-group">
                <li class="list-group-item d-flex justify-content-between align-items-start p-1">
                    <div><i class="{st}"></i> Pengajuan {str(row['jenis']).upper()}</div>
                </li>
                <li class="list-group-item d-flex justify-content-start align-items-center p-1">{row['keterangan']}</li>"""
            
            ul += durasi
            ul += f'<li class="list-group-item d-flex justify-content-between align-items-center p-1 {warna_status} font-weight-bolder"><span>Status:</span><span>{status_approval}</span></li>'
            ul += f'<li class="list-group-item d-flex justify-content-between align-items-center p-1 {warna_status} font-weight-bolder"><span>Catatan atasan:</span><span>{catatan_atasan}</span></li>'
            ul += '</ul>'

            item = row.drop(['mulai_dt', 'sampai_dt']).to_dict()
            item['daftar'] = escape(ul)
            data_list.append(item)

        # 6. Format JSON Response
        return JsonResponse({
            "draw": int(request.GET.get('draw', 1)),
            "recordsTotal": len(data_list),
            "recordsFiltered": len(data_list),
            "data": data_list
        })

    def get_radius_koordinat(self, request, decoded_payload):
        date = request.GET.get("tgl", datetime.now().strftime("%Y-%m-%d"))

        # 1. Query Data Absensi
        query_loc_abs = """
            SELECT fn_userid,
                SUBSTRING_INDEX(fc_tempat->>'$.in', ' ', 1) AS fc_tempat_in,
                SUBSTRING_INDEX(fc_posisi->>'$.in', ' ', 1) AS fc_posisi_in_lati,
                SUBSTRING_INDEX(fc_posisi->>'$.in', ' ', -1) AS fc_posisi_in_long
            FROM gishrdpayroll.absensi_dps a
            WHERE fd_tanggal >= %s AND fd_tanggal <= %s 
            AND fc_jenis IN ('absen') and stat <> 'd'
        """
        raw_loc_abs = DatabaseConnector.execute_raw_sql(
            query=query_loc_abs, params=[date, date], using="payroll"
        )
        df_loc_abs = pd.DataFrame(raw_loc_abs)
        
        # Cek jika data absensi kosong
        if df_loc_abs.empty:
            return JsonResponse([], safe=False)

        list_toko = list_to_sql_in(df_loc_abs, "fc_tempat_in")

        # 2. Query Data Toko Target Multiple
        query_loc_target = f"""
            SELECT 
                fc_codecus,
                SUBSTRING_INDEX(fv_coordinate, ' ', 1) AS fc_posisi_target_lati,
                SUBSTRING_INDEX(fv_coordinate, ' ', -1) AS fc_posisi_target_long,
                fn_radius
            FROM gishrdpayroll.koordinat_toko 
            WHERE fc_codecus IN ({list_toko})
        """
        raw_loc_target = DatabaseConnector.execute_raw_sql(
            query=query_loc_target, using="payroll"
        )
        df_loc_target = pd.DataFrame(raw_loc_target)

        if df_loc_target.empty:
            return JsonResponse([], safe=False)

        # 3. MERGE / JOIN DataFrame Absensi dan Target Toko
        # Menggabungkan data berdasarkan Relasi: fc_tempat_in == fc_codecus
        df_merged = pd.merge(
            df_loc_abs,
            df_loc_target,
            left_on="fc_tempat_in",
            right_on="fc_codecus",
            how="inner",  # Gunakan 'left' jika ingin menyimpan absensi yang tokonya tidak ketemu di master
        )

        # 4. Fungsi Kalkulasi Jarak Per Baris
        def calculate_distance(row):
            try:
                # Titik Aktual (User)
                absens_coords = (
                    float(row["fc_posisi_in_lati"]),
                    float(row["fc_posisi_in_long"]),
                )
                # Titik Acuan (Toko)
                target_coords = (
                    float(row["fc_posisi_target_lati"]),
                    float(row["fc_posisi_target_long"]),
                )

                # Hitung jarak (dalam meter)
                return geodesic(absens_coords, target_coords).meters
            except (ValueError, TypeError):
                return None  # Jika ada koordinat yang null atau bukan angka

        # Tambahkan kolom hasil hitung jarak
        df_merged["jarak_meter"] = df_merged.apply(calculate_distance, axis=1)
        df_merged["jarak_meter"] = (
            np.floor(df_merged["jarak_meter"]).astype("Int64")
        ).fillna(0)
        # Optional: Filter atau beri flag status jika > radius aman (misal 100m)
        # df_merged['is_within_radius'] = df_merged['jarak_meter'] <= 100

        # 5. Konversi ke JSON Response
        result = df_merged.sort_values(by='jarak_meter',ascending=False).to_dict(orient="records")
        return JsonResponse(result, safe=False)    
    
    def get_data_lembur(self, request,decoded_payload):
        user_id = request.GET.get('userid',decoded_payload.get('userid'))
        date=request.GET.get('tgl',datetime.now().strftime('%Y-%m-%d'))
        query_handle_toko = """
            SELECT DISTINCT u.fn_userid, ifnull(e.fn_cus_stay,0) fn_cusid, fv_nameemployees fv_nama
            FROM user_tb u
            JOIN employees_tb e ON u.fn_userid = e.fn_userid
            JOIN t_aksesusercust t ON u.fn_userid = t.fn_userid
            JOIN customer_tb c ON t.fn_cusid = c.fn_cusid
            JOIN t_aksesusercust ts ON c.fn_cusid = ts.fn_cusid
            WHERE u.fn_userlevel IN (1, 8, 13) AND e.fn_iddepart IN (8, 20)
            AND e.fc_hold = 'F' AND u.fc_hold = 'F' AND c.fc_hold = 'F' AND ts.fn_userid = %s
            ORDER BY c.fn_cusid;
        """
        raw_data = DatabaseConnector.execute_raw_sql(
            query=query_handle_toko, params=[user_id], using='payroll'
            )
        df_toko_by_user=pd.DataFrame(raw_data)
        list_toko=list_to_sql_in(df_toko_by_user,'fn_cusid')
        query_roster = f"""
            SELECT fn_userid, ifnull(a.fn_cusid,0) fn_cusid, ifnull(fv_shift_name,fc_status) as fv_shift_name,
            fc_status,fd_schedule_date,c.fn_shift_id,
            fd_start_time, fd_end_time
            FROM absensi_roster a
            left join customer_shifts b on a.fn_shift_version_id=b.fn_shift_version_id
            left join shifts_tb c on a.fn_shift_id=c.fn_shift_id or b.fn_shift_id=c.fn_shift_id
            left join shift_segments d on a.fn_shift_id=d.fn_shift_id or b.fn_shift_id=d.fn_shift_id
            
            WHERE a.fn_cusid IN ({list_toko}) and a.fd_schedule_date = %s

        """
        query_params = [date]
        raw_data = DatabaseConnector.execute_raw_sql(
            query=query_roster,
            params=query_params,
            using='payroll',
            p_queri=False
        )
        df_roster = pd.DataFrame(raw_data)
        if df_roster.empty:
                df_roster=pd.DataFrame(columns=['fn_userid','fn_cusid', 'fv_shift_name','fn_shift_version_id',
                                            'fv_shift_code', 'fd_start_time', 'fd_end_time',
                                            'fn_shift_id'])
        df_roster = df_toko_by_user.merge(df_roster,how='left',on=['fn_userid','fn_cusid']).fillna('-')
        list_toko = list_to_sql_in(df_roster, 'fn_cusid')
        df_cus = df__customers_pb_('fn_cusid,concat(fc_code," ",fv_toko) fv_namecus', where=f'and fn_cusid in({list_toko})')
        df_roster = df_cus.merge(df_roster, on='fn_cusid', how='right').fillna('-')
        df_roster["is_empty_a"] = df_roster["fv_shift_name"] == "-"

        # 2. Urutkan berdasarkan kolom bantuan terlebih dahulu, baru diikuti col_b, col_c, col_d
        # Karena False (0) < True (1), maka data yang TIDAK KOSONG akan naik ke atas secara otomatis
        df_roster = df_roster.sort_values(
            by=["is_empty_a", "fv_namecus", "fv_nama", "fv_shift_name"],
            ascending=[True, True, True, True],  # Sesuaikan urutan ASC/DESC di sini
        ).drop(columns=["is_empty_a"])
        result = {
            "sum_abs": df_roster.to_dict(orient='records'),
            # "raw2": df_absens_sum.to_dict(orient='records'),
            # "raw3": df_absens_sum_left.to_dict(orient='records'),
            # "sum_abs": db_results.sort_values('fc_code').to_dict(orient='records'),
        }
        return JsonResponse(result, safe=False)
    
class UserModelHelper:

    @staticmethod
    def get_user(request_data):
        """
        Mengecek apakah user yang login terdaftar sebagai Manager atau Supervisor.
        Menggunakan DatabaseConnector dengan opsi fetch_one=True.
        """
        username = request_data.get('username', '')
        password = request_data.get('password', '')
        
        # Enkripsi password string ke MD5 hexdigest
        password_md5 = hashlib.md5(password.encode('utf-8')).hexdigest()

        # 1. Cek ketersediaan di tabel Manager
        query_manager = """
            SELECT * FROM employees_manager_tb 
            WHERE fv_username = %s AND fv_password_web = %s AND fc_hold = 'F' 
            LIMIT 1
        """
        # Menembak ke db 'payroll' dan mengembalikan 1 dictionary objek (atau None)
        is_manager = DatabaseConnector.execute_raw_sql(
            query=query_manager, 
            params=[username, password_md5], 
            using='payroll', 
            fetch_one=True
        )

        if is_manager:
            return is_manager

        # 2. Cek ketersediaan di tabel Supervisor jika Manager tidak ditemukan
        query_supervisor = """
            SELECT u.fn_userlevel, u.fv_username, e.*
            FROM employees_tb e
            INNER JOIN user_tb u ON e.fn_userid = u.fn_userid
            WHERE e.fc_hold = 'F' 
                AND u.fc_hold = 'F'
                AND u.fv_username = %s 
                AND u.fv_password_web = %s
                AND e.fn_iddepart IN (8, 20)
                AND u.fn_userlevel IN (2, 12)
            LIMIT 1
        """
        is_supervisor = DatabaseConnector.execute_raw_sql(
            query=query_supervisor, 
            params=[username, password_md5], 
            using='payroll', 
            fetch_one=True
        )

        if is_supervisor:
            return is_supervisor

        # Pengganti return [] di Laravel jika akun benar-benar tidak ditemukan
        return {}

    @staticmethod
    def get_store_by_user(request_data, session_data):
        """
        Mengambil daftar toko (customer) berdasarkan role level dari session newUser.
        Mengembalikan list of dict (Fetch All).
        """
        # Membaca session userlevel dari struktur nested 'newUser'
        level = session_data.get('newUser', {}).get('userlevel')
        user_id = request_data.get('user')

        # Kondisi Jika user login adalah Supervisor (2 atau 12)
        if level in [2, 12]:
            query = """
                SELECT c.fn_cusid, c.fc_codecus, c.fv_namecus
                FROM customer_tb c
                INNER JOIN t_aksesusercust t ON t.fn_cusid = c.fn_cusid
                WHERE t.fn_userid = %s
            """
            params = [user_id]
        
        # Kondisi Jika user login adalah Manager
        else:
            query = """
                SELECT c.fn_cusid, c.fc_codecus, c.fv_namecus
                FROM customer_tb c
                INNER JOIN t_aksesusercust t ON t.fn_cusid = c.fn_cusid
                INNER JOIN t_aksesusermanager tm ON t.fn_userid = tm.fn_supervisorid
                WHERE tm.fn_managerid = %s
                ORDER BY c.fc_codecus ASC
            """
            params = [user_id]

        # Menggunakan parameter default fetch_one=False untuk mengambil semua toko (Array List)
        stores = DatabaseConnector.execute_raw_sql(
            query=query, 
            params=params, 
            using='payroll', 
            fetch_one=False
        )
        return stores

    @staticmethod
    def get_supervisor_by_manager(request_data):
        """
        Mengambil daftar Supervisor di bawah koordinasi Manager tertentu.
        Mengembalikan list of dict (Fetch All).
        """
        user_id = request_data.get('user')

        query = """
            SELECT e.fn_userid, e.fv_nameemployees
            FROM t_aksesusermanager t
            INNER JOIN employees_tb e ON t.fn_supervisorid = e.fn_userid
            WHERE t.fn_managerid = %s
            ORDER BY e.fv_nameemployees ASC
        """
        
        supervisors = DatabaseConnector.execute_raw_sql(
            query=query, 
            params=[user_id], 
            using='payroll', 
            fetch_one=False
        )
        return supervisors
    
class UserDataAPIView(APIView):

    def get(self, request):
        # Proteksi Session
        if not request.GET.get('userid'):
            return Response({'error': 'Unauthorized'}, status=status.HTTP_401_UNAUTHORIZED)

        type_param = request.GET.get('type')

        try:
            # Contoh Route untuk ?type=toko
            if type_param == 'toko':
                # Mengirim parameter GET (?user=3009) dan object session ke helper
                data = UserModelHelper.get_store_by_user(request.GET, request.session)
                return Response(data, status=status.HTTP_200_OK)

            # Contoh Route untuk ?type=spv
            elif type_param == 'spv':
                data = UserModelHelper.get_supervisor_by_manager(request.GET)
                return Response(data, status=status.HTTP_200_OK)

            else:
                return Response({'error': 'Type tidak dikenali'}, status=status.HTTP_400_BAD_REQUEST)

        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    

@method_decorator(csrf_exempt, name='dispatch')
class DraftLemburView(View):
    """
    Class khusus untuk merepresentasikan dan mengelola data Draf Lembur (Keranjang).
    Menangani GET (list), POST (tambah), PATCH (checkout massal), dan DELETE (hapus draf).
    """
    def dispatch(self, request, *args, **kwargs):
        auth_header = request.headers.get('Authorization')
        
        # 1. Validasi keberadaan header Authorization
        if not auth_header:
            return JsonResponse({"error": "Akses ditolak, token tidak ditemukan!"}, status=401)
        
        # 2. Validasi format "Bearer <token>" untuk mencegah IndexError
        header_parts = auth_header.split(" ")
        if len(header_parts) != 2 or header_parts[0].lower() != 'bearer':
            return JsonResponse({"error": "Format token salah! Gunakan 'Bearer <token>'"}, status=401)
            
        token = auth_header.split(" ")[1].strip()
        # 3. Proses Decode JWT dengan Error Handling yang menyeluruh
        try:
            # Gunakan settings.SECRET_KEY agar selalu sinkron dengan config Django Anda
            decoded_payload = jwt.decode(token, settings.SECRET_KEY, algorithms=["HS256"])
            
            # 4. Validasi isi payload
            if 'userid' not in decoded_payload:
                return JsonResponse({'error': 'Unauthorized, ID Pengguna tidak ditemukan di dalam token'}, status=401)
                
            # Lampirkan data user ke dalam object request agar bisa diakses di method GET, POST, dll.
            request.user_id = decoded_payload['userid']
            request.user_payload = decoded_payload

        except jwt.ExpiredSignatureError:
            return JsonResponse({'error': 'Token sudah kedaluwarsa, silakan login kembali'}, status=401)
        except jwt.InvalidSignatureError:
            return JsonResponse({'error': 'Signature verification failed, token tidak valid'}, status=401)
        except jwt.DecodeError:
            return JsonResponse({'error': 'Token rusak atau tidak dapat dibaca'}, status=401)
        except jwt.PyJWTError:
            return JsonResponse({'error': 'Terjadi kesalahan pada validasi token'}, status=401)

        return super().dispatch(request, *args, **kwargs)

    def get(self, request):
        """Menampilkan/Mengambil data draf lembur aktif untuk Datatables."""
        user_id_pembuat = request.user_id
        
        user_id_pembuat= request.GET.get('userid',request.user_id)
        query_get_data = """
            SELECT 
                ct.fc_codecus, 
                ct.fv_namecus,
                da.*,
                et.fv_nameemployees
            FROM draft_approval AS da
            INNER JOIN customer_tb AS ct ON da.fn_cusid = ct.fn_cusid
            INNER JOIN employees_tb AS et ON da.fn_userid = et.fn_userid
            WHERE da.fn_userid_pembuat = %s 
              AND da.fc_hold = 'F' 
              AND da.fc_uploaded = 'F'
            ORDER BY da.fn_id DESC
        """
        draft_list = DatabaseConnector.execute_raw_sql(query_get_data
                                                       , [user_id_pembuat], using='payroll')
        return JsonResponse(draft_list, safe=False)

    def post(self, request):
        """Menambahkan data draf lembur baru (Bulk Insert)."""
        user_id_pembuat = request.user_id
        now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        spg_lembur = request.POST.getlist('spgLembur[]') or request.POST.getlist('spgLembur')
        toko_spg = request.POST.get('tokoSpg')
        tgl_lembur_spg = request.POST.get('tglLemburSpg')
        keterangan_lembur = request.POST.get('keteranganLemburSpg')
        
        try:
            dt_parsed = datetime.strptime(tgl_lembur_spg, '%Y-%m-%d').strftime('%Y-%m-%d %H:%M:%S')
        except ValueError:
            dt_parsed = now_str

        if spg_lembur:
            value_placeholders = []
            params = []
            
            for val in spg_lembur:
                value_placeholders.append("(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
                params.extend([
                    val, toko_spg, 'lembur', tgl_lembur_spg, dt_parsed, dt_parsed,
                    keterangan_lembur, 'otomatis approve by system karena pengajuan oleh spv',
                    user_id_pembuat, now_str
                ])
                
            query_insert = f"""
                INSERT INTO draft_approval 
                (fn_userid, fn_cusid, fc_jenis, fd_tanggal, fd_mulai, fd_sampai, fv_keterangan, fv_note, fn_userid_pembuat, fd_create)
                VALUES {', '.join(value_placeholders)}
            """
            DatabaseConnector.execute_raw_sql(query_insert, params, using='payroll')
            
        return HttpResponse('ok')

    def patch(self, request):
        """Checkout massal: Memindahkan seluruh draf aktif ke antrean approval."""
        user_id_pembuat = request.user_id
        now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        with transaction.atomic():
            query_select = """
                SELECT fn_id, fn_userid, fd_tanggal, fc_jenis, fd_mulai, fd_sampai, fv_keterangan, fv_note, fc_approve
                FROM draft_approval
                WHERE fn_userid_pembuat = %s AND fc_hold = 'F' AND fc_uploaded = 'F'
            """
            uploaded_draft = DatabaseConnector.execute_raw_sql(query_select, [user_id_pembuat], using='payroll')

            if uploaded_draft:
                value_placeholders = []
                insert_params = []
                draft_ids = []

                for row in uploaded_draft:
                    draft_ids.append(row['fn_id'])
                    value_placeholders.append("(%s, %s, %s, %s, %s, %s, %s, %s, %s)")
                    insert_params.extend([
                        row['fn_userid'], row['fd_tanggal'], row['fc_jenis'], 
                        row['fd_mulai'], row['fd_sampai'], row['fv_keterangan'], 
                        row['fv_note'], row['fc_approve'], now_str
                    ])

                query_insert_approval = f"""
                    INSERT INTO approval_dps_dev 
                    (fn_userid, fd_tanggal, fc_jenis, fd_mulai, fd_sampai, fv_keterangan, fv_note, fc_approve, fd_create)
                    VALUES {', '.join(value_placeholders)}
                """
                DatabaseConnector.execute_raw_sql(query_insert_approval, insert_params, using='payroll')

                id_placeholders = ', '.join(['%s'] * len(draft_ids))
                query_update_draft = f"""
                    UPDATE draft_approval 
                    SET fd_update = %s, fc_uploaded = 'T' 
                    WHERE fn_id IN ({id_placeholders})
                """
                update_params = [now_str] + draft_ids
                DatabaseConnector.execute_raw_sql(query_update_draft, update_params, using='payroll')

        return HttpResponse('ok')

    def delete(self, request):
        """Soft delete satu item draf dari keranjang."""
        now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        try:
            data = json.loads(request.body)
            id_draft = data.get('id')
        except json.JSONDecodeError:
            id_draft = request.GET.get('id')

        with transaction.atomic():
            query_delete = """
                UPDATE draft_approval 
                SET fd_delete = %s, fc_hold = 'T' 
                WHERE fn_id = %s
            """
            DatabaseConnector.execute_raw_sql(query_delete, [now_str, id_draft], using='payroll')

        return HttpResponse('ok')


@method_decorator(csrf_exempt, name='dispatch')
class ActionApprovalView(View):
    def _catch_error_details(self, exception_obj):
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
    """
    Class khusus untuk menangani Aksi Persetujuan/Penolakan Pengajuan oleh Manager.
    Menangani POST untuk pemrosesan status 'Y' atau 'N' ke tabel absensi final.
    """
    def dispatch(self, request, *args, **kwargs):
        auth_header = request.headers.get('Authorization')
        
        # 1. Validasi keberadaan header Authorization
        if not auth_header:
            return JsonResponse({"error": "Akses ditolak, token tidak ditemukan!"}, status=401)
        
        # 2. Validasi format "Bearer <token>"
        header_parts = auth_header.split(" ")
        if len(header_parts) != 2 or header_parts[0].lower() != 'bearer':
            return JsonResponse({"error": "Format token salah! Gunakan 'Bearer <token>'"}, status=401)
            
        token = header_parts[1]
        
        # 3. Proses Decode JWT
        try:
            decoded_payload = jwt.decode(token, settings.SECRET_KEY, algorithms=["HS256"])
            
            # 4. Validasi isi payload
            if 'userid' not in decoded_payload:
                return JsonResponse({'error': 'Unauthorized, ID Pengguna tidak ditemukan di dalam token'}, status=401)
                
            request.user_id = decoded_payload['userid']
            request.user_payload = decoded_payload

        except jwt.ExpiredSignatureError:
            return JsonResponse({'error': 'Token sudah kedaluwarsa, silakan login kembali'}, status=401)
        except jwt.InvalidSignatureError:
            return JsonResponse({'error': 'Signature verification failed, token tidak valid'}, status=401)
        except jwt.DecodeError:
            return JsonResponse({'error': 'Token rusak atau tidak dapat dibaca'}, status=401)
        except jwt.PyJWTError:
            return JsonResponse({'error': 'Terjadi kesalahan pada validasi token'}, status=401)

        return super().dispatch(request, *args, **kwargs)
    
    def post(self, request):
        user_id_pembuat = request.user_id

        # 1. Parsing data JSON dari request body raw
        try:
            data = json.loads(request.body)
        except json.JSONDecodeError:
            return JsonResponse({
                "status": "error",
                "message": "Format JSON tidak valid atau body kosong."
            }, status=400)

        # 2. Ambil data dari object JSON
        action_raw = data.get('action', '')
        note = data.get('note', '')
        action = re.split(r'_', action_raw) if action_raw else []
        if len(action) < 2:
            return JsonResponse({
                "status": "error", 
                "message": "Format parameter 'action' tidak valid. Gunakan format 'nomor_status' (contoh: 123_Y)"
            }, status=400)

        fn_no = action[0]
        status_approve = action[1]
        now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        try:
            with transaction.atomic():
                query_update = """
                    UPDATE approval_dps_dev 
                    SET fv_note = %s, fc_approve = %s, fd_update = %s, stat = 'u'
                    WHERE fn_no = %s
                """
                DatabaseConnector.execute_raw_sql(query_update, [note, status_approve, now_str, fn_no], using='payroll',p_queri=False)

                if status_approve == 'Y':
                    query_select = """
                        SELECT fn_userid, fd_mulai, fd_sampai, fc_jenis 
                        FROM approval_dps_dev 
                        WHERE fn_no = %s
                    """
                    pengajuan_list = DatabaseConnector.execute_raw_sql(query_select, [fn_no], using='payroll',p_queri=False)
                    if not pengajuan_list:
                        return JsonResponse({
                            "status": "error", 
                            "message": f"Data pengajuan dengan nomor {fn_no} tidak ditemukan."
                        }, status=404)
                    pengajuan = pengajuan_list[0]
                    fc_jenis = pengajuan['fc_jenis']
                    user_id = pengajuan['fn_userid']
                    start_date = datetime.strptime(str(pengajuan['fd_mulai'])[:10], '%Y-%m-%d')
                    end_date = datetime.strptime(str(pengajuan['fd_sampai'])[:10], '%Y-%m-%d')
                    query_roster_update = """
                    UPDATE absensi_roster 
                    SET fn_shift_id = 0, fn_shift_version_id = 0
                    WHERE fn_userid = %s and fd_schedule_date = %s
                    """
                    DatabaseConnector.execute_raw_sql(query_roster_update, [user_id, start_date], using='payroll',p_queri=False)

                    if fc_jenis not in ['lembur', 'tukar off', 'other']:
                        current_date = start_date
                        while current_date <= end_date:
                            tgl_str = current_date.strftime('%Y-%m-%d')
                            json_payload = json.dumps({'in': fc_jenis, 'out': fc_jenis})

                            query_insert_absen = """
                                INSERT INTO absensi_dps 
                                (fn_userid, fd_tanggal, fc_status, fc_jenis, fc_tempat, fc_jam, fc_foto, fc_shift, fd_create, stat)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """
                            params_insert = [
                                user_id, tgl_str, fc_jenis, fc_jenis, 
                                json_payload, json_payload, json_payload, json_payload, 
                                now_str, 'i'
                            ]
                            DatabaseConnector.execute_raw_sql(query_insert_absen, params_insert, using='payroll',p_queri=False)
                            current_date += timedelta(days=1)

                    if fc_jenis in ['tukar off', 'other']:
                        tgl_off_str = end_date.strftime('%Y-%m-%d')
                        json_off_payload = json.dumps({'in': 'off', 'out': 'off'})

                        query_insert_off = """
                            INSERT INTO absensi_dps 
                            (fn_userid, fd_tanggal, fc_status, fc_jenis, fc_tempat, fc_jam, fc_foto, fc_shift, fd_create, stat)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """
                        params_off = [
                            user_id, tgl_off_str, 'off', 'off', 
                            json_off_payload, json_off_payload, json_off_payload, json_off_payload, 
                            now_str, 'i'
                        ]
                        DatabaseConnector.execute_raw_sql(query_insert_off, params_off, using='payroll',p_queri=False)

            return JsonResponse({
                "status": "success",
                "message": f"Aksi pengajuan nomor {fn_no} berhasil diproses dengan status '{status_approve}'."
            }, status=200)

        except Exception as e:
            # Kembalikan error detail ke client agar mudah didebug
            return self._catch_error_details(e)

@method_decorator(csrf_exempt, name='dispatch')       
class NoLogAbsensiView(View):
    """
    Mengelola pengajuan absensi mandiri tanpa log harian (Off, Lembur, Cuti, dll).
    Padanan dari public function nolog(Request $request) di Laravel.
    """
    def _catch_error_details(self, exception_obj):
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
    def dispatch(self, request, *args, **kwargs):
        auth_header = request.headers.get('Authorization')
        
        # 1. Validasi keberadaan header Authorization
        if not auth_header:
            return JsonResponse({"error": "Akses ditolak, token tidak ditemukan!"}, status=401)
        
        # 2. Validasi format "Bearer <token>" untuk mencegah IndexError
        header_parts = auth_header.split(" ")
        if len(header_parts) != 2 or header_parts[0].lower() != 'bearer':
            return JsonResponse({"error": "Format token salah! Gunakan 'Bearer <token>'"}, status=401)
            
        token = header_parts[1].strip()
        
        # 3. Proses Decode JWT dengan Error Handling yang menyeluruh
        try:
            # Gunakan settings.SECRET_KEY agar selalu sinkron dengan config Django Anda
            decoded_payload = jwt.decode(token, settings.SECRET_KEY, algorithms=["HS256"])
            
            # 4. Validasi isi payload
            if 'userid' not in decoded_payload:
                return JsonResponse({'error': 'Unauthorized, ID Pengguna tidak ditemukan di dalam token'}, status=401)
                
            # Lampirkan data user ke dalam object request agar bisa diakses di method GET, POST, dll.
            request.user_id = decoded_payload['userid']
            request.user_level = decoded_payload['userlevel']
            request.user_payload = decoded_payload
            
        except jwt.ExpiredSignatureError:
            return JsonResponse({'error': 'Token sudah kedaluwarsa, silakan login kembali'}, status=401)
        except jwt.InvalidSignatureError:
            return JsonResponse({'error': 'Signature verification failed, token tidak valid'}, status=401)
        except jwt.DecodeError:
            return JsonResponse({'error': 'Token rusak atau tidak dapat dibaca'}, status=401)
        except jwt.PyJWTError:
            return JsonResponse({'error': 'Terjadi kesalahan pada validasi token'}, status=401)

        return super().dispatch(request, *args, **kwargs)

    def post(self, request):
        
        from .utils import kirim_notifikasi_ke_spv
        user_id = request.user_id
        user_level = request.user_level
        now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Cek tipe konten request (Raw JSON vs Form-Data)
        is_json = request.content_type == 'application/json'
        
        if is_json:
            try:
                json_data = json.loads(request.body)
                jenis = json_data.get('jenis', '').lower()
            except json.JSONDecodeError:
                return JsonResponse({"status": "error", "message": "Format JSON tidak valid"}, status=400)
        else:
            jenis = request.POST.get('jenis', '').lower()

        # =========================================================================
        # PERCABANGAN KHUSUS: LEMBUR BULK (RAW JSON) -> Sudah aman di akhir
        # =========================================================================
        if jenis == 'lembur':
            if not is_json:
                return JsonResponse({"status": "error", "message": "Untuk pengajuan lembur, wajib menggunakan Raw JSON"}, status=400)
            return self._handle_lembur_bulk(request, json_data, now_str)

        # =========================================================================
        # PROSES FORM-DATA SEPERTI BIASA (UNTUK JENIS NON-LEMBUR: OFF, CUTI, SAKIT, TUKAR OFF)
        # =========================================================================
        tanggal_req = request.POST.get('tanggal', '')
        keterangan = request.POST.get('keterangan', '')
        spg_tukar = request.POST.get('spgTukar')
        
        if spg_tukar:
            keterangan += f", tukar dengan {spg_tukar}"

        response_msg = 'Pengajuan telah dibuat, silahkan lihat menu Rekap - Pengajuan'
        
        # is_email_sent = kirim_notifikasi_ke_spv(user_id, jenis, tanggal_req, keterangan)
        # print(is_email_sent)
        # if not is_email_sent:
        #     response_msg += " (Catatan: Notifikasi email ke SPV gagal terkirim, mohon konfirmasi manual)."
        #     return JsonResponse({
        #         "status": "Failed", 
        #         "message": response_msg, 
        #     }, status=200)
        try:
            data_to_insert = []

            # -----------------------------------------------------------------
            # KONDISI A: JIKA JENIS ADALAH 'off'
            # -----------------------------------------------------------------
            if jenis == 'off':
                tgl_sekarang = datetime.now().strftime('%Y-%m-%d')
                tgl = request.POST.get('tanggal', tgl_sekarang)
                user_id = request.POST.get('spg', user_id)
                query_toko = """
                    SELECT fn_cusid FROM t_aksesusercust 
                    WHERE fn_userid = %s 
                    ORDER BY fn_idusercust ASC LIMIT 1
                """
                toko_res = DatabaseConnector.execute_raw_sql(query_toko, [user_id], using='payroll', fetch_one=True)
                toko_id = toko_res['fn_cusid'] if toko_res else 0

                json_payload = json.dumps({'in': jenis, 'out': jenis})
                
                fn_toko_in = toko_id if user_level in [1, 8, 13] else 0

                data_to_insert.append({
                    'table': 'absensi_dps',
                    'columns': '(fn_userid, fd_tanggal, fc_jenis, fc_status, fc_shift, fc_tempat, fc_jam, fc_foto, fn_toko_in, fd_create, stat)',
                    'placeholders': '%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s',
                    'params': [user_id, tgl, jenis, jenis, json_payload, json_payload, json_payload, json_payload, fn_toko_in, now_str, 'i']
                })
                response_msg = 'Anda off hari ini'

            # -----------------------------------------------------------------
            # KONDISI B: JIKA JENIS ADALAH 'tukar off'
            # -----------------------------------------------------------------
            elif jenis == 'tukar off':
                mulai_raw = request.POST.get('mulai', now_str)
                sampai_raw = request.POST.get('sampai', now_str)
                try:
                    mulai_dt = datetime.strptime(mulai_raw, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')
                    sampai_dt = datetime.strptime(sampai_raw, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')
                except ValueError:
                    mulai_dt, sampai_dt = mulai_raw, sampai_raw

                data_to_insert.append({
                    'table': 'approval_dps_dev',
                    'columns': '(fn_userid, fc_jenis, fd_tanggal, fd_mulai, fd_sampai, fv_keterangan, fv_attachment, fc_approve, fd_create, stat)',
                    'placeholders': '%s, %s, %s, %s, %s, %s, %s, %s, %s, %s',
                    'params': [user_id, jenis, tanggal_req, mulai_dt, sampai_dt, keterangan, '-', 'F', now_str, 'i']
                })

            # -----------------------------------------------------------------
            # KONDISI C: MULTI-DAY LOOP (Cuti / Sakit, dll)
            # -----------------------------------------------------------------
            else:  
                mulai_raw = request.POST.get('mulai', tanggal_req)
                sampai_raw = request.POST.get('sampai', tanggal_req)
                start_date = datetime.strptime(mulai_raw[:10], '%Y-%m-%d')
                end_date = datetime.strptime(sampai_raw[:10], '%Y-%m-%d')

                current_date = start_date
                while current_date <= end_date:
                    tgl_loop_str = current_date.strftime('%Y-%m-%d %H:%M:%S')
                    data_to_insert.append({
                        'table': 'approval_dps_dev',
                        'columns': '(fn_userid, fc_jenis, fd_tanggal, fd_mulai, fd_sampai, fv_keterangan, fv_attachment, fc_approve, fd_create, stat)',
                        'placeholders': '%s, %s, %s, %s, %s, %s, %s, %s, %s, %s',
                        'params': [user_id, jenis, tanggal_req, tgl_loop_str, tgl_loop_str, keterangan, '-', 'F', now_str, 'i']
                    })
                    current_date += timedelta(days=1)

            # =========================================================================
            # EKSEKUSI DATABASE TRANSACTION (ALL-OR-NOTHING)
            # =========================================================================
            with transaction.atomic():
                for item in data_to_insert:
                    query_bulk = f"INSERT INTO {item['table']} {item['columns']} VALUES ({item['placeholders']})"
                    DatabaseConnector.execute_raw_sql(query_bulk, item['params'], using='payroll')

            # =========================================================================
            # KIRIM NOTIFIKASI EMAIL DI AKHIR (SETELAH DATA SAKTI/PASTI COMMIT DI DB)
            # =========================================================================
            # Jenis 'off' langsung disetujui sistem (tidak perlu notifikasi persetujuan ke SPV)
            if jenis != 'off':
                is_email_sent = kirim_notifikasi_ke_spv(user_id, jenis, tanggal_req, keterangan)
                
                # Jika email gagal, data tidak dibatalkan karena sudah commit, tapi beri warning di response
                if not is_email_sent:
                    response_msg += " (Catatan: Notifikasi email ke SPV gagal terkirim, mohon konfirmasi manual)."

            return JsonResponse({
                "status": "success", 
                "message": response_msg, 
                "data": {"jenis": jenis, "tanggal": tanggal_req}
            }, status=200)

        except Exception as e:
            print(f" [TRACK RESULT] -> !!! TRANSACTION FAILED (Rollbacked) !!!")
            return self._catch_error_details(e)
        
    def _handle_lembur_bulk(self, request, json_data, now_str):
        """
        Menangani input list lembur per user dengan jam/keterangan variatif (Raw JSON).
        Format JSON:
        {
            "jenis": "lembur",
            "tanggal": "2026-07-10",
            "data": [
                {"mulai_jam": "17:30", "sampai_jam": "21:30", "durasi": 240, "keterangan": "...", "users": 101},
                {"mulai_jam": "16:00", "sampai_jam": "20:00", "durasi": 240, "keterangan": "...", "users": 102}
            ]
        }
        """
        tanggal_req = json_data.get('tanggal', '')
        list_lembur = json_data.get('data', [])

        if not list_lembur or not isinstance(list_lembur, list):
            return JsonResponse({"status": "error", "message": "Data lembur berupa list 'data' tidak boleh kosong"}, status=400)

        # 1. EKSTRAK SEMUA USER ID UNTUK CEK DUPLIKAT SECARA BULK
        # Ambil field 'users' dari tiap item di dalam list
        user_ids = [item.get('users') for item in list_lembur if item.get('users')]
        
        if not user_ids:
            return JsonResponse({"status": "error", "message": "Tidak ada User ID valid ditemukan dalam data"}, status=400)

        try:
            # 2. VALIDASI DOUBLE INPUT (Bulk Check - 1x Query)
            placeholders_users = ", ".join(["%s"] * len(user_ids))
            query_cek = f"""
                SELECT fn_userid FROM approval_dps_dev 
                WHERE fn_userid IN ({placeholders_users}) 
                  AND DATE(fd_tanggal) = %s 
                  AND fc_jenis = 'lembur' 
                  AND fc_approve <> 'N'
            """
            params_cek = user_ids + [tanggal_req]
            cek_res = DatabaseConnector.execute_raw_sql(query_cek, params_cek, using='payroll')
            
            if cek_res:
                user_duplikat = [row['fn_userid'] for row in cek_res]
                return JsonResponse({
                    "status": "error",
                    "message": f"Pengajuan dibatalkan. User ID {user_duplikat} sudah memiliki pengajuan lembur pada tanggal {tanggal_req}."
                }, status=400)

            # 3. EKSEKUSI DATABASE TRANSACTION (All-or-Nothing)
            inserted_ids = []

            with transaction.atomic():
                for item in list_lembur:
                    u_id = item.get('users')
                    mulai_jam = item.get('mulai_jam', '00:00')
                    sampai_jam = item.get('sampai_jam', '00:00')
                    durasi_menit = item.get('durasi', 0)
                    keterangan = item.get('keterangan', '')

                    mulai_dt = f"{tanggal_req} {mulai_jam}:00"
                    sampai_dt = f"{tanggal_req} {sampai_jam}:00"

                    # Insert ke tabel approval_dps_dev
                    query_insert_approval = """
                        INSERT INTO approval_dps_dev 
                        (fn_userid, fc_jenis, fd_tanggal, fd_mulai, fd_sampai, fv_keterangan, fv_attachment, fc_approve, fd_create, stat)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    params_approval = [u_id, 'lembur', tanggal_req, mulai_dt, sampai_dt, keterangan, '-', 'F', now_str, 'i']
                    DatabaseConnector.execute_raw_sql(query_insert_approval, params_approval, using='payroll')

                    # Ambil Last Insert ID untuk tabel durasi
                    res_id = DatabaseConnector.execute_raw_sql("SELECT LAST_INSERT_ID() AS last_id", [], using='payroll', fetch_one=True)
                    if res_id and res_id.get('last_id'):
                        lembur_id = res_id['last_id']
                        inserted_ids.append(lembur_id)
                        
                        # Insert ke t_durasi_lembur
                        query_durasi = """
                            INSERT INTO t_durasi_lembur (fn_no_approval, fn_durasi_menit, fd_create) 
                            VALUES (%s, %s, %s)
                        """
                        DatabaseConnector.execute_raw_sql(query_durasi, [lembur_id, durasi_menit, now_str], using='payroll')

            # =========================================================================
            # 4. KIRIM EMAIL DI AKHIR (SETELAH DATA PASTI COMMIT DI DB)
            # =========================================================================
            from .utils import kirim_notifikasi_ke_spv
            
            email_gagal_users = []
            for item in list_lembur:
                u_id = item.get('users')
                keterangan = item.get('keterangan', '')
                
                is_sent = kirim_notifikasi_ke_spv(u_id, 'lembur', tanggal_req, keterangan)
                if not is_sent:
                    email_gagal_users.append(u_id)

            response_msg = f"Berhasil membuat pengajuan lembur untuk {len(user_ids)} karyawan."
            if email_gagal_users:
                response_msg += f" Catatan: Gagal mengirimkan email untuk User ID: {email_gagal_users}, data di sistem tetap aman."

            return JsonResponse({
                "status": "success",
                "message": response_msg,
                "data": {
                    "jenis": "lembur",
                    "tanggal": tanggal_req,
                    "total_user": len(user_ids),
                    "approval_ids": inserted_ids
                }
            }, status=200)

        except Exception as e:
            print(" [TRACK RESULT] -> !!! BULK LEMBUR VARIANT TRANSACTION FAILED (Rollbacked) !!!")
            return self._catch_error_details(e)
        
@method_decorator(csrf_exempt, name='dispatch')
class AttendanceLogView(View):
    # """
    # Menangani Log Absensi Check In dan Check Out (Staff & Lapangan).
    # Padanan dari public function log() di Laravel.
    # """
    def _catch_error_details(self, exception_obj):
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
        print(full_traceback_formatted)
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
    def dispatch(self, request, *args, **kwargs):
        auth_header = request.headers.get('Authorization')
        
        # 1. Validasi keberadaan header Authorization
        if not auth_header:
            return JsonResponse({"error": "Akses ditolak, token tidak ditemukan!"}, status=401)
        
        # 2. Validasi format "Bearer <token>" untuk mencegah IndexError
        header_parts = auth_header.split(" ")
        if len(header_parts) != 2 or header_parts[0].lower() != 'bearer':
            return JsonResponse({"error": "Format token salah! Gunakan 'Bearer <token>'"}, status=401)
            
        token = header_parts[1]
        
        # 3. Proses Decode JWT dengan Error Handling yang menyeluruh
        try:
            # Gunakan settings.SECRET_KEY agar selalu sinkron dengan config Django Anda
            decoded_payload = jwt.decode(token, settings.SECRET_KEY, algorithms=["HS256"])
            
            # 4. Validasi isi payload
            if 'userid' not in decoded_payload:
                return JsonResponse({'error': 'Unauthorized, ID Pengguna tidak ditemukan di dalam token'}, status=401)
                
            # Lampirkan data user ke dalam object request agar bisa diakses di method GET, POST, dll.
            request.user_id = decoded_payload['userid']
            request.user_level = decoded_payload['userlevel']
            request.user_payload = decoded_payload

        except jwt.ExpiredSignatureError:
            return JsonResponse({'error': 'Token sudah kedaluwarsa, silakan login kembali'}, status=401)
        except jwt.InvalidSignatureError:
            return JsonResponse({'error': 'Signature verification failed, token tidak valid'}, status=401)
        except jwt.DecodeError:
            return JsonResponse({'error': 'Token rusak atau tidak dapat dibaca'}, status=401)
        except jwt.PyJWTError:
            return JsonResponse({'error': 'Terjadi kesalahan pada validasi token'}, status=401)

        return super().dispatch(request, *args, **kwargs)
    
    def post(self, request):
        # -------------------------------------------------------------------------
        # BANNER UTAMA TRACK LOG (Terlihat di Terminal saat Postman Klik Send)
        # -------------------------------------------------------------------------
        print("\n" + "="*60)
        print(" [TRACK TEST] OUTBOUND REQUEST FROM POSTMAN DETECTED")
        print("="*60)
        # 1. Proteksi Session
        if 'userlogin' not in request.user_payload:
            # Menggunakan Django messages framework untuk session flash alert
            return JsonResponse({"error": "Verifikasi userlogin gagal"}, status=401)

        user_id = request.user_id
        user_level = int(request.user_level)
        
        # Ambil koordinat GPS
        capture_coordinate = request.POST.get('captureCoordinate')
        if not capture_coordinate:
            capture_coordinate = '-'

        # Deteksi tipe file upload (File dari input form vs Base64 string dari webcam canvas)
        if request.FILES.get('capture'):
            uploaded_file = request.FILES['capture']
            # Anda bisa simpan sementara file ini ke folder temporary jika dibutuhkan
            file_ref = uploaded_file 
            img_type = 'file'
        else:
            file_ref = request.POST.get('capture')
            img_type = 'base64'

        last_no = int(request.POST.get('lastno', 0))
        now = datetime.now()
        today_str = now.strftime('%Y-%m-%d')
        now_str = now.strftime('%Y-%m-%d %H:%M:%S')
        time_str = now.strftime('%H:%M:%S')
        # Penentuan nama berkas wepb absensi (Sesuai kondisi tanggal tahun 2026 di script asal)
        filename = f"{now.strftime('%Y/%m/%d')}/{user_id}.webp"
        
        # Penentuan folder folder path berdasarkan level staff (level 0) atau lapangan
        if user_level == 0:
            path_file = f"staff/log_in/{filename}" if last_no == 0 else f"staff/log_out/{filename}"
        else:
            path_file = f"log_in/{filename}" if last_no == 0 else f"log_out/{filename}"

        # Parsing data toko / customer
        tempat_keterangan = request.POST.get('tempatKeterangan')
        tempat_raw = request.POST.get('tempat', '')
        toko = [0, '']
        if not tempat_keterangan and ' ' in tempat_raw:
            toko = tempat_raw.strip().split(' ', 1)

        kegiatan = request.POST.get('kegiatan')
        shift_input = request.POST.get('shift', '')
        keterangan_penugasan = request.POST.get('keteranganPenugasanToko')
        print(f" -> Log Mode Status: {'CHECK-IN (Masih Baru)' if last_no == 0 else f'CHECK-OUT (Update ID: {last_no})'}")
        
        path_file = handle_image_upload(file_ref, path_file, img_type)

        try:
            with transaction.atomic():
                
                # =============================================================
                # ALUR 1: LOG IN / CHECK IN (last_no == 0) -> INSERT BARU
                # =============================================================
                if last_no == 0:
                    # Inisialisasi struktur JSON awal untuk pilar absensi
                    tempat_json = json.dumps({'in': tempat_keterangan if tempat_keterangan else toko[1], 'out': ''})
                    jam_json = json.dumps({'in': time_str, 'out': ''})
                    foto_json = json.dumps({'in': path_file, 'out': ''})
                    
                    shift_in_val = kegiatan if kegiatan else shift_input
                    shift_out_val = shift_input if user_level in [1, 8, 13] else ''
                    shift_json = json.dumps({'in': shift_in_val, 'out': shift_out_val})
                    
                    posisi_json = json.dumps({'in': capture_coordinate, 'out': capture_coordinate})
                    penjualan_json = json.dumps({'qty': 0, 'value': 0})

                    if user_level == 0:
                        # Insert ke tabel absensi internal kantor
                        query_in_staff = """
                            INSERT INTO absensi_staff 
                            (fn_userid, fd_tanggal, fc_status, fc_tempat, fc_jam, fc_foto, fc_shift, fc_posisi, fd_create, stat)
                            VALUES (%s, %s, 'in', %s, %s, %s, %s, %s, %s, 'i')
                        """
                        params_in = [user_id, today_str, tempat_json, jam_json, foto_json, shift_json, posisi_json, now_str]
                        DatabaseConnector.execute_raw_sql(query_in_staff, params_in,using='payroll')
                    else:
                        # Insert ke tabel absensi lapangan (SPG / MD / SPV)
                        query_in_fields = """
                            INSERT INTO absensi_dps 
                            (fn_userid, fd_tanggal, fc_status, fc_jenis, fc_tempat, fc_jam, fc_foto, fc_shift, fc_posisi, fn_toko_in, fd_create, fc_penjualanharian, stat, fv_penugasan_in)
                            VALUES (%s, %s, 'in', %s, %s, %s, %s, %s, %s, %s, %s, %s, 'i', %s)
                        """
                        toko_in_id = 0 if tempat_keterangan else toko[0]
                        params_in = [
                            user_id, today_str, request.POST.get('jenis', ''), tempat_json, jam_json, 
                            foto_json, shift_json, posisi_json, toko_in_id, now_str, penjualan_json, 
                            keterangan_penugasan if keterangan_penugasan else None
                        ]
                        DatabaseConnector.execute_raw_sql(query_in_fields, params_in,using='payroll')

                # =============================================================
                # ALUR 2: LOG OUT / CHECK OUT (last_no > 0) -> UPDATE DATA LAMA
                # =============================================================
                else:
                    out_tempat_val = tempat_keterangan if tempat_keterangan else toko[1]
                    out_shift_val = kegiatan if kegiatan else shift_input

                    if user_level == 0:
                        # Update data pulang untuk staff kantor memanfaatkan JSON_SET MySQL
                        query_out_staff = """
                            UPDATE absensi_staff 
                            SET fc_status = 'out',
                                fc_tempat = JSON_SET(fc_tempat, '$.out', %s),
                                fc_jam = JSON_SET(fc_jam, '$.out', %s),
                                fc_foto = JSON_SET(fc_foto, '$.out', %s),
                                fc_shift = JSON_SET(fc_shift, '$.out', %s),
                                fc_posisi = JSON_SET(fc_posisi, '$.out', %s),
                                fd_update = %s,
                                stat = 'u'
                            WHERE fn_no = %s
                        """
                        params_out = [out_tempat_val, time_str, path_file, out_shift_val, capture_coordinate, now_str, last_no]
                        DatabaseConnector.execute_raw_sql(query_out_staff, params_out,using='payroll')
                    else:
                        # Mengambil objek data inputan nominal penjualan harian di postman/form
                        penjualanharian_raw = request.POST.get('penjualanharian', '{"qty":0,"value":0}')
                        
                        query_out_fields = """
                            UPDATE absensi_dps 
                            SET fc_status = 'out',
                                fc_tempat = JSON_SET(fc_tempat, '$.out', %s),
                                fc_jam = JSON_SET(fc_jam, '$.out', %s),
                                fc_foto = JSON_SET(fc_foto, '$.out', %s),
                                fc_shift = JSON_SET(fc_shift, '$.out', %s),
                                fc_posisi = JSON_SET(fc_posisi, '$.out', %s),
                                fn_toko_out = %s,
                                fc_penjualanharian = %s,
                                fd_update = %s,
                                stat = 'u',
                                fv_penugasan_out = %s
                            WHERE fn_no = %s
                        """
                        toko_out_id = 0 if tempat_keterangan else toko[0]
                        params_out = [
                            out_tempat_val, time_str, path_file, out_shift_val, capture_coordinate,
                            toko_out_id, penjualanharian_raw, now_str, 
                            keterangan_penugasan if keterangan_penugasan else None, last_no
                        ]
                        DatabaseConnector.execute_raw_sql(query_out_fields, params_out,using='payroll')

                # Jalankan fungsi pemrosesan gambar latar belakang
                # handle_image_upload(file_ref, path_file, img_type)
            print(" [TRACK RESULT] -> Transaksi SQL BERHASIL (Committed)")
            print("="*60 + "\n")
            return JsonResponse({
                "status": "success",
                "message": "Absensi berhasil dicatat",
                "redirect_to": "/absensi-dev",  # Opsional, jika frontend butuh info redirect
                "mode": "CHECK-IN" if last_no == 0 else "CHECK-OUT"
            }, status=200)

        except Exception as e:
            # Jika transaksi SQL gagal, kembalikan ke home dengan flash alert bahwasanya koordinat/foto corrupt
            print(f" [TRACK RESULT] -> !!! Transaksi SQL GAGAL (Rollbacked) !!!")
            print("="*60 + "\n")
            return self._catch_error_details(e)

@method_decorator(csrf_exempt, name='dispatch')
class RosterView(DataLayerMixin,View):   
    def _catch_error_details(self, exception_obj):
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
    
    def _calculate_duration_minutes(self, start_time_str, end_time_str):
        """
        Menghitung durasi dalam menit antara start_time dan end_time.
        Otomatis handle kasus shift yang melewati tengah malam (cross midnight),
        misal start 22:00 - end 06:00 -> dianggap 8 jam, bukan negatif.
        """
        fmt = "%H:%M:%S"
        start_dt = datetime.strptime(start_time_str, fmt)
        end_dt = datetime.strptime(end_time_str, fmt)

        if end_dt <= start_dt:
            # Shift melewati tengah malam, tambahkan 1 hari ke end_time
            end_dt += timedelta(days=1)

        duration = end_dt - start_dt
        return int(duration.total_seconds() // 60)
    def _replace_shift_segments(self, shift_id, segments, db_target, current_time):
        """
        Helper: hapus semua segment lama lalu insert ulang segment baru.
        Mengembalikan total durasi (menit) dari seluruh segment.
        """
        query_delete_segments = "DELETE FROM shift_segments WHERE fn_shift_id = %s"
        DatabaseConnector.execute_raw_sql(query=query_delete_segments, params=[shift_id], using=db_target)

        total_duration = 0
        for idx, seg in enumerate(segments, start=1):
            start_time = seg.get('fd_start_time')
            end_time = seg.get('fd_end_time')
            duration_minutes = self._calculate_duration_minutes(start_time, end_time)
            total_duration += duration_minutes

            query_insert_segment = """
                INSERT INTO shift_segments (
                    fn_shift_id, fn_segment_order, fd_start_time, fd_end_time, 
                    fn_duration_minutes, fd_created_at
                )
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            DatabaseConnector.execute_raw_sql(
                query=query_insert_segment,
                params=[shift_id, idx, start_time, end_time, duration_minutes, current_time],
                using=db_target
            )

        return total_duration
    
    

    def _resolve_shift_prefix_and_split(self, shift_name):
        SHIFT_CODE_MAP = {
        'shift pagi':   'P',
        'shift middle': 'M',
        'shift siang':  'S',
        'shift split':  'Z',
        }
        """
        Menentukan fv_shift_code dan fc_is_split otomatis berdasarkan fv_shift_name.
        Hanya ada 4 jenis shift yang valid: Pagi, Middle, Siang, Split.
        Pencocokan dilakukan via keyword (case-insensitive, partial match),
        jadi "Shift Pagi", "shift pagi reguler", "PAGI" semuanya cocok ke 'pagi'.
        """
        if not shift_name:
            return None, None

        name_lower = shift_name.lower()
        for keyword, code in SHIFT_CODE_MAP.items():
            if keyword in name_lower:
                is_split = 'T' if keyword == 'split' else 'F'
                return code, is_split

        return None, None  # tidak cocok salah satu dari 4 jenis
    
    def _generate_next_shift_code(self, prefix, db_target):
        """
        Mencari kode terakhir dengan prefix tertentu (misal 'P'), lalu generate
        nomor urut berikutnya dengan format 3 digit, contoh: P001 -> P002.
        Kalau belum ada sama sekali untuk prefix tersebut, mulai dari 001.
        """
        query_last_code = """
            SELECT fv_shift_code FROM shifts_tb 
            WHERE fv_shift_code LIKE %s 
            ORDER BY fv_shift_code DESC LIMIT 1
        """
        result = DatabaseConnector.execute_raw_sql(
            query=query_last_code, params=[f"{prefix}%"], using=db_target, fetch_one=True
        )

        if not result:
            next_number = 1
        else:
            last_code = result['fv_shift_code']  # contoh: "P001"
            last_number_str = last_code[len(prefix):]  # ambil bagian angkanya saja
            try:
                next_number = int(last_number_str) + 1
            except ValueError:
                next_number = 1  # fallback kalau format kode lama tidak sesuai pola

        return f"{prefix}{next_number:03d}"  # 1 -> "001", 12 -> "012"
    
    def _check_existing_shift_by_time(self, segments, db_target):
        """
        Mengecek apakah salah satu segment jam yang dikirim sudah terdaftar
        di shift lain (manapun, tidak peduli prefix). Mengembalikan info shift
        pertama yang ditemukan bentrok, atau None kalau semua jam masih bebas.

        Hanya cek terhadap shift yang TIDAK di-hold (fc_hold = 'F'), karena shift
        yang sudah di-hold dianggap tidak aktif dan jamnya boleh dipakai ulang.
        """
        for seg in segments:
            start_time = seg.get('fd_start_time')
            end_time = seg.get('fd_end_time')

            if not start_time or not end_time:
                continue

            query_check_time = """
                SELECT s.fv_shift_code, s.fv_shift_name, ss.fd_start_time, ss.fd_end_time
                FROM shift_segments ss
                JOIN shifts_tb s ON s.fn_shift_id = ss.fn_shift_id
                WHERE ss.fd_start_time = %s AND ss.fd_end_time = %s AND s.fc_hold = 'F'
                LIMIT 1
            """
            jam_shift = DatabaseConnector.execute_raw_sql(
                query=query_check_time, params=[start_time, end_time], using=db_target, fetch_one=True
            )

            if jam_shift:
                return jam_shift  # langsung return begitu ketemu 1 yang bentrok

        return None
    
    def get(self, request, **kwargs):
        # print('test')
        # print("=== GET Request to LoginAPIView ===")
        # print(f"Full path: {request.get_full_path()}")
        # print(f"GET params: {dict(request.GET)}")
        # print(f"Origin: {request.headers.get('Origin', 'No Origin')}")
        auth_header = request.headers.get('Authorization')
        
        print('test')
        # print(auth_header)
        if not auth_header:
            return JsonResponse({"message": "Akses ditolak, token tidak ditemukan!"}), 401
        # print(auth_header)
        token = auth_header.split(" ")[1].strip()
        try:
            # print('test')
            decoded_payload = jwt.decode(token, settings.SECRET_KEY, algorithms=["HS256"])
            # print('test')
        except InvalidSignatureError:
            # Kembalikan respon 401 Unauthorized alih-alih membiarkan server error 500
            return JsonResponse({'error': 'Token tidak valid / Signature failed'}, status=401)
        except ExpiredSignatureError:
            return JsonResponse({'error': 'Token sudah kedaluwarsa'}, status=401)
        
        # print('test')
        # 🟢 AKTIFKAN PRINT INI UNTUK DEBUGGING PAYLOAD
        try:
            type_param = request.GET.get('type', 'roster_type')
            print(type_param)
            # user_id = decoded_payload.get('userid')
            # user_level = decoded_payload.get('userlevel')
            # user_name = decoded_payload.get('userlogin')
            # user_full_name = decoded_payload.get('fullname')
            
            # if not user_id:
            #     return JsonResponse({'error': 'Unauthorized. Silakan login kembali.'}, status=401)

            # months = {
            #     'January': 'Januari', 'February': 'Februari', 'March': 'Maret', 'April': 'April',
            #     'May': 'Mei', 'June': 'Juni', 'July': 'Juli', 'August': 'Agustus',
            #     'September': 'September', 'October': 'Oktober', 'November': 'November', 'December': 'Desember'
            # }

            # Routing penanganan berdasarkan parameter ?type=...
            print(type_param)
            if type_param == 'roster_by_user':
                return self.get_roster_by_user(request,decoded_payload)
            elif type_param == 'roster_by_customer':
                return self.get_roster_by_customer(request,decoded_payload)
            elif type_param == 'sp_cus_roster':
                return self.get_sp_cus_roster(request,decoded_payload)
            elif type_param == 'shifts_code':
                return self.get_shifts_code()
            else:
                return JsonResponse({'error': f"Type '{type_param}' tidak ditemukan"}, status=400)

        except Exception as e:
            return self._catch_error_details(e)
            
    def post(self, request, *args, **kwargs):
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
        # Membaca type parameter baik dari form-data maupun JSON body
        type_param = request.POST.get('type')
        # print(type_param)
        if not type_param:
            try:
                # print("BODY RAW:", request.body)
                # Fallback jika payload dikirim sebagai JSON raw body
                body_data = json.loads(request.body.decode('utf-8'))
                # print("BODY JSON:", body_data)
                type_param = body_data.get('type')
                print("TYPE:", type_param)
            except Exception:
                pass
                
        try:
            if type_param == 'upsert_customer_shift':
                return self.post_upsert_customer_shift(request,decoded_payload, *args, **kwargs)
            elif type_param == 'regist_shift':
                return self.post_register_shift(request,decoded_payload, *args, **kwargs)
            elif type_param == 'upsert_roster':
                return self.post_roster_insert(request,decoded_payload, *args, **kwargs)
            elif type_param == 'upsert_customer_shift_special':
                return self.post_upsert_customer_shift_special(request,decoded_payload, *args, **kwargs)
            # Jika type_param tidak cocok dengan route manapun
            return JsonResponse({'success': False, 'message': f"Method type '{type_param}' tidak valid atau tidak ditemukan."}, status=400)
            
        except Exception as e:
            print(traceback.format_exc())
            return self._catch_error_details(e)
        
    def get_roster_by_user(self,request,decoded_payload):
        # test = request.GET.get('test', '')
        user_id = request.GET.get('userid',decoded_payload.get('userid'))
        checked = request.GET.get('default',1)
        bulan_raw = request.GET.get('bulan', '')
        tahun_raw = request.GET.get('tahun', '')
        summary=request.GET.get('sum', 0)
        start=request.GET.get('start')
        end=request.GET.get('end')
        cusid = request.GET.get('toko')
        mgr = request.GET.get('manager')
        spv = request.GET.get('supervisor')
        provinsi = request.GET.get('provinsi')
        kota = request.GET.get('kota')
        spg = request.GET.get('spg')
        if bulan_raw and tahun_raw:
            bulan = int(bulan_raw)
            tahun = int(tahun_raw)
            
            # 2. Tentukan tanggal mulai
            start_date = f"{tahun}-{bulan:02d}-01"
            
            # 3. Ambil hari terakhir pada bulan tersebut menggunakan calendar.monthrange
            # monthrange mengembalikan tuple: (hari_pertama_dalam_minggu, jumlah_hari_dalam_bulan)
            _, jumlah_hari = calendar.monthrange(tahun, bulan)
            
            # 4. Tentukan tanggal akhir
            end_date = f"{tahun}-{bulan:02d}-{jumlah_hari}"
            
            # Jika Anda membutuhkan end_date dalam format Pandas Timestamp:
            # end_date = pd.to_datetime(f"{tahun}-{bulan:02d}-{jumlah_hari}")

            print(f"Start: {start_date}, End: {end_date}")
        elif start and end:
            start_date=start
            end_date=end
        else:
            print("Parameter bulan atau tahun tidak valid/kosong")
            list_toko=''
        df_cus=self.df_cust_(
            colom='fn_cusid,fv_toko namecus, fc_code codecus, fv_spv, fv_manager, fv_provinsi, fv_kota',
            queri=False,
            ** {'fn_cusid':cusid, 'fv_spv':spv, 'fv_manager':mgr,'fv_provinsi':provinsi,'fv_kota':kota}
            )
        if not df_cus.empty:
            list_toko=list_to_sql_in(df_cus,'fn_cusid')
            list_toko=f"and fn_cus_stay in ({list_toko})"
        params_emp=[user_id]
        if spg: 
            params_emp.append(int(spg))
            spg_filter=f"AND e.fn_userid = %s" 
        else:spg_filter=''
        query_handle_toko = f"""
            SELECT DISTINCT u.fn_userid, ifnull(e.fn_cus_stay,0) fn_cusid
            FROM user_tb u
            JOIN employees_tb e ON u.fn_userid = e.fn_userid
            JOIN t_aksesusercust t ON u.fn_userid = t.fn_userid
            JOIN customer_tb c ON t.fn_cusid = c.fn_cusid
            JOIN t_aksesusercust ts ON c.fn_cusid = ts.fn_cusid
            WHERE u.fn_userlevel IN (1, 8, 13) AND e.fn_iddepart IN (8, 20)
            AND e.fc_hold = 'F' AND u.fc_hold = 'F' AND c.fc_hold = 'F' AND ts.fn_userid = %s 
            {list_toko} {spg_filter}
            ORDER BY c.fn_cusid;
        """
        raw_data = DatabaseConnector.execute_raw_sql(
            query=query_handle_toko, params=params_emp, using='payroll'
            )
        df_toko_by_user=pd.DataFrame(raw_data)
        list_toko=list_to_sql_in(df_toko_by_user,'fn_cusid')
        range_tgl_clause = "AND DATE(fd_effective_from) <= DATE(%s) and( DATE(%s) <= DATE(fd_effective_until) OR fd_effective_until IS NULL)"
        query_roster = f"""
            SELECT fn_userid, ifnull(a.fn_cusid,0) fn_cusid, ifnull(fv_shift_name,fc_status) as fv_shift_name,
            fc_status,fd_schedule_date,c.fn_shift_id,fv_week_type,fv_shift_code, fd_start_time, fd_end_time
            FROM absensi_roster a
            left join customer_shifts b on a.fn_shift_version_id=b.fn_shift_version_id
            left join shifts_tb c on a.fn_shift_id=c.fn_shift_id or b.fn_shift_id=c.fn_shift_id
            JOIN shift_segments s ON a.fn_shift_id = s.fn_shift_id
            WHERE a.fn_cusid IN ({list_toko}) {range_tgl_clause} or a.fn_shift_version_id = 0

        """
        query_params = [end_date, start_date]
        raw_data = DatabaseConnector.execute_raw_sql(
            query=query_roster,
            params=query_params,
            using='payroll',
            p_queri=False
        )
        df_roster = pd.DataFrame(raw_data)
        if df_roster.empty:
                df_roster=pd.DataFrame(columns=['fn_userid','fn_cusid', 'fv_shift_name','fn_shift_version_id',
                                            'fv_shift_code', 'fd_start_time', 'fd_end_time',
                                            'fn_shift_id','fv_week_type'])
        df_roster = df_toko_by_user.merge(df_roster,how='left',on=['fn_userid','fn_cusid'])
        list_user=list_to_sql_in(df_roster,'fn_userid')
        
        # -------------------------------------------------------------
        # PROSES PEMBUATAN KALENDER LENGKAP PER USER & TOKO
        # -------------------------------------------------------------
        
        # 1. Pastikan kolom tanggal di df_roster bertipe datetime agar match saat merge
        if not df_roster.empty:
            df_roster['fd_schedule_date'] = pd.to_datetime(df_roster['fd_schedule_date'])
            # Pastikan tipe data id konsisten (misal: int)
            df_roster['fn_userid'] = df_roster['fn_userid'].astype(int)
            df_roster['fn_cusid'] = df_roster['fn_cusid'].astype(int)
        df_roster[['fv_shift_name','fc_status']]=df_roster[['fv_shift_name','fc_status']].fillna('Off')
        # 2. Buat deret tanggal dalam bulan tersebut
        dates = pd.date_range(start=start_date, end=end_date, freq='D')
        df_calendar = pd.DataFrame({'tgl': dates})
        
        # 3. Ambil daftar unik user dan toko yang terlibat dari roster awal
        # (atau jika ingin mengacu pada semua toko milik user, gunakan df_toko_by_user)
        if not df_roster.empty:
            df_pairs = df_roster[['fn_userid', 'fn_cusid']].drop_duplicates()
        else:
            # Fallback jika roster kosong, pakai user saat ini dan semua tokonya
            df_pairs = df_toko_by_user[['fn_userid', 'fn_cusid']].drop_duplicates()

        # 4. Lakukan CROSS JOIN (MultiIndex) untuk membuat kombinasi lengkap: User x Toko x Tanggal
        df_matrix = df_pairs.merge(df_calendar, how='cross')
        
        # 5. Gabungkan matriks kalender lengkap dengan data roster asli (LEFT JOIN)
        df_hasil = pd.merge(
            df_matrix,
            df_roster,
            left_on=['fn_userid', 'fn_cusid', 'tgl'],
            right_on=['fn_userid', 'fn_cusid', 'fd_schedule_date'],
            how='left'
        )
        df_hasil[['fv_shift_name', 'fc_status']] = df_hasil[['fv_shift_name', 'fc_status']].fillna('-')
        df_cus = df__customers_pb_('fn_cusid,fc_code,fv_toko',where=f'and fn_cusid IN ({list_toko})')
        df_hasil = pd.merge(df_cus,df_hasil,on='fn_cusid',how='right')
        query_employee = f"""
                    SELECT fn_userid, fv_nameemployees AS fullname FROM employees_tb user
                    WHERE user.fn_userid in ({list_user})
                """
        raw_data = DatabaseConnector.execute_raw_sql(query=query_employee,using='payroll',p_queri=False)
        df_employee = pd.DataFrame(raw_data)
        df_hasil = pd.merge(df_employee,df_hasil,on='fn_userid',how='right')
        # print(df_hasil.info())
        query_absen = f"""
                            SELECT fn_no, fn_userid, fd_tanggal tgl, fc_jenis absensi FROM absensi_dps
                            WHERE fc_jenis <>'absen' and fd_tanggal between %s and %s and stat <> 'd'
                        """
        
        query_params = [start_date, end_date]
        raw_data = DatabaseConnector.execute_raw_sql(query=query_absen,params=query_params,using='payroll',p_queri=False)
        df_absen = pd.DataFrame(raw_data)
        if not df_absen.empty:
            df_absen['tgl'] = pd.to_datetime(df_absen['tgl'])
            df_absen['fn_userid'] = df_absen['fn_userid'].astype(int)
            
            # OPTIMASI 1: Hilangkan duplikat di df_absen per user per hari sebelum di-merge
            # agar tidak merusak struktur perkalian matriks toko.
            df_absen = df_absen.drop_duplicates(subset=['fn_userid', 'tgl'], keep='first')
        df_hasil = pd.merge(df_hasil,df_absen,on=['fn_userid','tgl'],how='left')

        # 6. Mengisi data yang kosong (NaN) dengan 'OFF' atau status default
        df_hasil['fv_shift_name'] = df_hasil['fv_shift_name'].fillna('-')
        df_hasil['fc_status'] = df_hasil['fc_status'].fillna('-') # Opsional jika butuh status
        
        # SINKRONISASI KOLOM TANGGAL HASIL MERGE
        df_hasil['fd_schedule_date'] = df_hasil['fd_schedule_date'].fillna(df_hasil['tgl'])
        df_hasil['fd_schedule_date'] = pd.to_datetime(df_hasil['fd_schedule_date']).dt.date
        df_hasil.drop(columns=['tgl'], inplace=True) # Buang kolom tgl temporer

        # Jika ada data absensi riil (Sakit/Izin/dll), tampilkan itu. Jika tidak, pakai fv_shift_name.
        df_hasil[['absensi','fn_no']] = df_hasil[['absensi','fn_no']].fillna('')
        df_hasil['status_tampilan'] = df_hasil.apply(
            lambda row: row['absensi'] if row['absensi'] != '' else row['fv_shift_name'], axis=1
        )
        is_absen_terisi = df_hasil['absensi'] != ''
        
        df_hasil['fv_shift_name'] = df_hasil['absensi'].where(is_absen_terisi, df_hasil['fv_shift_name'])
        df_hasil['fv_shift_code'] = df_hasil['absensi'].where(is_absen_terisi, df_hasil['fv_shift_code'])
        df_hasil['fc_status'] = df_hasil['absensi'].where(is_absen_terisi, df_hasil['fc_status'])
        
        # Kolom status_tampilan tetap mendengarkan logika yang sama
        df_hasil['status_tampilan'] = df_hasil['absensi'].where(is_absen_terisi, df_hasil['fv_shift_name'])
        # Urutkan berdasarkan User, Toko, lalu Tanggal agar rapi
        df_hasil = df_hasil.sort_values(by=['fn_userid', 'fn_cusid', 'fd_schedule_date']).reset_index(drop=True)
        # Pastikan kolom fd_schedule_date sudah berupa string
        if int(summary) == 1:
            # 1. Normalisasi kategori shift
            def kumpulkan_kategori_shift(shift_name):
                s = str(shift_name).upper()
                if 'PAGI' in s:
                    return 'shift_pagi'
                elif 'SIANG' in s or 'SORE' in s:
                    return 'shift_siang'
                elif 'MIDDLE' in s or 'MID' in s:
                    return 'shift_middle'
                elif any(k in s for k in ['OFF', 'CUTI', 'SAKIT', 'LIBUR', 'IZIN', '-']):
                    return 'shift_off'
                else:
                    return 'shift_lainnya'

            df_hasil['kategori_shift'] = df_hasil['fv_shift_name'].apply(kumpulkan_kategori_shift)

            # 2. Pivot Table per Toko & Tanggal
            df_pivot = df_hasil.pivot_table(
                index=['fn_cusid', 'fc_code', 'fv_toko', 'fd_schedule_date'],
                columns='kategori_shift',
                values='fn_userid',
                aggfunc='nunique',
                fill_value=0
            ).reset_index()

            # 3. Pastikan kolom shift lengkap
            expected_cols = ['shift_pagi', 'shift_siang', 'shift_middle', 'shift_off']
            for col in expected_cols:
                if col not in df_pivot.columns:
                    df_pivot[col] = 0

            # 4. Kalkulasi Total
            df_pivot['total_user_masuk'] = df_pivot['shift_pagi'] + df_pivot['shift_siang'] + df_pivot['shift_middle']
            df_pivot['total_user'] = df_pivot['total_user_masuk'] + df_pivot['shift_off']

            # Pastikan format tanggal string (YYYY-MM-DD)
            df_pivot['fd_schedule_date'] = df_pivot['fd_schedule_date'].astype(str)

            # =========================================================================
            # PERBAIKAN: Ubah semua kolom angka NumPy/Pandas menjadi Python int murni
            # =========================================================================
            num_cols = expected_cols + ['total_user_masuk', 'total_user']
            for col in num_cols:
                df_pivot[col] = df_pivot[col].astype(int)

            # Convert DataFrame ke dictionary native Python dulu agar aman dari numpy.int64
            records = df_pivot.to_dict(orient='records')

            # 5. Transformasi ke format Nested JSON
            # Kita buat dictionary sementara untuk mengelompokkan toko
            summary_dict = {}

            for row in records:
                key = (row['fn_cusid'], row['fc_code'], row['fv_toko'])
                
                if key not in summary_dict:
                    summary_dict[key] = {
                        "fn_cusid": row['fn_cusid'],
                        "fc_code": row['fc_code'],
                        "fv_toko": row['fv_toko'],
                        "cus_summary": {}
                    }
                
                tgl = row['fd_schedule_date']
                summary_dict[key]["cus_summary"][tgl] = {
                    "shift_pagi": int(row['shift_pagi']),
                    "shift_siang": int(row['shift_siang']),
                    "shift_middle": int(row['shift_middle']),
                    "shift_off": int(row['shift_off']),
                    "total_user_masuk": int(row['total_user_masuk']),
                    "total_user": int(row['total_user'])
                }

            # Ambil list hasil grouping-nya
            summary_nested = list(summary_dict.values())

            result = {
                "summary": summary_nested
            }
            return JsonResponse(result, safe=False)
        
        df_hasil['fd_schedule_date'] = df_hasil['fd_schedule_date'].astype(str)

        group_cols = ['fn_userid', 'fullname', 'fn_cusid', 'fc_code', 'fv_toko']
        # Urutkan berdasarkan User, Toko, lalu Tanggal agar rapi
        df_hasil = df_hasil.sort_values(by=['fn_userid', 'fn_cusid', 'fd_schedule_date']).reset_index(drop=True)
        # 1. Pastikan kolom detail shift yang baru terisi nilai default jika NaN
        shift_cols_detail = ['fv_shift_code', 'fd_start_time', 'fd_end_time']
        for col in shift_cols_detail:
            if col in df_hasil.columns:
                df_hasil[col] = df_hasil[col].fillna('-')

        # 2. Sinkronkan format tanggal & hapus duplikat sebelum grouping
        df_hasil['fd_schedule_date'] = df_hasil['fd_schedule_date'].astype(str)
        group_cols = ['fn_userid', 'fullname', 'fn_cusid', 'fc_code', 'fv_toko']
        kolom_nested = group_cols + [
            'fd_schedule_date', 
            'fv_shift_name', 
            'fv_shift_code', 
            'fd_start_time', 
            'fd_end_time', 
            'fc_status', 
            'fn_no', 
            'absensi'
        ]

        # Safety deduplication agar tidak crash dengan orient='index'
        df_hasil_clean = df_hasil[kolom_nested].drop_duplicates(
            subset=group_cols + ['fd_schedule_date'], 
            keep='first'
        )

        # 3. MAPPING NESTED DICT (Sertakan fv_shift_code, fd_start_time, fd_end_time)
        df_nested = (
            df_hasil_clean.groupby(group_cols)
            .apply(lambda x: x.set_index('fd_schedule_date')[[
                'fv_shift_name', 
                'fv_shift_code', 
                'fd_start_time', 
                'fd_end_time', 
                'fc_status', 
                'fn_no', 
                'absensi'
            ]].to_dict(orient='index'), include_groups=False)
            .reset_index(name='roster')
            .sort_values(by=['fv_toko', 'fullname'], ascending=True)
        )
        
        group_cols = ['fn_cusid', 'fc_code', 'fv_toko']
    
        if int(checked) == 1:
            # 1. Ambil dataframe roster customer awal
            d_cus_rost_raw = self.get_roster_by_customer(request, decoded_payload, output='df')
            
            if not d_cus_rost_raw.empty:
                # 2. Lakukan Cross Join dengan kalender bulan ini
                # (Setiap baris shift akan direplikasi sebanyak jumlah hari dalam bulan tersebut)
                d_cus_rost_raw = d_cus_rost_raw.merge(df_calendar, how='cross')
                
                # 3. Handling tanggal efektif (jika '-' diubah jadi None/NaT agar mempermudah operasi datetime)
                d_cus_rost_raw['fd_effective_from'] = pd.to_datetime(d_cus_rost_raw['fd_effective_from'], errors='coerce')
                d_cus_rost_raw['fd_effective_until'] = pd.to_datetime(d_cus_rost_raw['fd_effective_until'], errors='coerce')
                
                # 4. Buat filter kondisi berdasarkan rentang tanggal efektif
                # Kondisi A: fd_effective_from kosong/'-' ATAU tanggal kalender >= fd_effective_from
                cond_from = d_cus_rost_raw['fd_effective_from'].isna() | (d_cus_rost_raw['tgl'] >= d_cus_rost_raw['fd_effective_from'])
                
                # Kondisi B: fd_effective_until kosong/'-' ATAU tanggal kalender <= fd_effective_until
                cond_until = d_cus_rost_raw['fd_effective_until'].isna() | (d_cus_rost_raw['tgl'] <= d_cus_rost_raw['fd_effective_until'])
                
                # Kondisi C: Cek kecocokan hari (WEEKDAY / WEEKEND)
                # tgl.dt.dayofweek >= 5 artinya hari Sabtu (5) atau Minggu (6)
                is_weekend_day = d_cus_rost_raw['tgl'].dt.dayofweek >= 5
                cond_week_type = (
                    ((d_cus_rost_raw['fv_week_type'].str.upper() == 'WEEKEND') & is_weekend_day) |
                    ((d_cus_rost_raw['fv_week_type'].str.upper() == 'WEEKDAY') & ~is_weekend_day)
                )
                
                # Gabungkan semua filter kueri
                df_filtered_rost = d_cus_rost_raw[cond_from & cond_until & cond_week_type].copy()
                
                # 5. Karena satu tanggal bisa memiliki history perubahan (overlap record jika data duplikat),
                # kita prioritaskan data shift versi terbaru atau yang memiliki detail lebih spesifik jika dibutuhkan.
                # Di sini kita amankan dengan drop_duplicates berdasarkan toko, tanggal, dan shift code.
                df_filtered_rost = df_filtered_rost.drop_duplicates(subset=['fn_cusid', 'tgl', 'fv_shift_code'], keep='first')
                
                # 6. Format ulang data agar struktur bersarang (nested dict) sesuai tanggal 'tgl'
                # Mengubah tanggal menjadi string 'YYYY-MM-DD' sebagai key
                df_filtered_rost['tgl_str'] = df_filtered_rost['tgl'].dt.date.astype(str)
                
                # Grouping bertingkat: Toko -> Tanggal -> Daftar Shift yang tersedia di tanggal tersebut
                kolom_rost_1 = ['fn_cusid', 'fc_code', 'fv_toko', 'tgl_str', 'fv_shift_code', 
                'fn_shift_version_id', 'fv_shift_name', 'fd_start_time', 'fd_end_time', 'fn_shift_id']
                d_cus_rost = (
                    df_filtered_rost[kolom_rost_1].groupby(['fn_cusid', 'fc_code', 'fv_toko'])
                    .apply(lambda x: x.groupby('tgl_str').apply(
                        lambda sub_x: sub_x.set_index('fv_shift_code')[[
                            'fn_shift_version_id', 
                            'fv_shift_name', 
                            'fd_start_time', 
                            'fd_end_time', 
                            'fn_shift_id'
                        ]].to_dict(orient='index')
                    ).to_dict())
                    .reset_index(name='cus_rost')
                )
            else:
                # Fallback jika data get_roster_by_customer kosong
                d_cus_rost = pd.DataFrame(columns=['fn_cusid', 'fc_code', 'fv_toko', 'cus_rost'])
        else:
             
            query_shift = f"""
                SELECT distinct s.fv_shift_code, s.fv_shift_name, ss.fd_start_time, ss.fd_end_time, ss.fn_shift_id
                FROM shift_segments ss
                JOIN shifts_tb s ON s.fn_shift_id = ss.fn_shift_id 
                where s.fv_shift_code not like "Z%%"
                order by fv_shift_name,fv_shift_code
            """
            raw_data = DatabaseConnector.execute_raw_sql(
                query=query_shift,
                using='payroll',
                p_queri=False
            )
            df_shift_all=pd.DataFrame(raw_data)
            df_toko_by_shift = df_hasil[['fn_cusid', 'fc_code', 'fv_toko']].drop_duplicates()
            df_toko_with_week = df_toko_by_shift.merge(df_calendar, how='cross')
            d_cus_rost = df_toko_with_week.merge(df_shift_all, how='cross')
            d_cus_rost['tgl_str'] = d_cus_rost['tgl'].dt.date.astype(str)
            d_cus_rost['fn_shift_version_id'] = None 
            kolom_rost_2 = ['fn_cusid', 'fc_code', 'fv_toko', 'tgl_str', 'fv_shift_code', 
                'fn_shift_version_id', 'fv_shift_name', 'fd_start_time', 'fd_end_time', 'fn_shift_id']
            d_cus_rost = (
                d_cus_rost[kolom_rost_2].groupby(['fn_cusid', 'fc_code', 'fv_toko'])
                .apply(lambda x: x.groupby('tgl_str').apply(
                    lambda sub_x: sub_x.set_index('fv_shift_code')[[
                        'fn_shift_version_id', 
                        'fv_shift_name', 
                        'fd_start_time', 
                        'fd_end_time', 
                        'fn_shift_id'
                    ]].to_dict(orient='index')
                ).to_dict())
                .reset_index(name='cus_rost')
            )
        result = {
            # "raw2": df_roster.to_dict(orient='records') if not df_roster.empty else [],
            "cus_shift": d_cus_rost.to_dict(orient='records'),
            "roster": df_nested.to_dict(orient='records'),
            # "raw2": df_sale_comp.to_dict(orient='records') if not df_sale_comp.empty else [],
            # "table_bazaar": df_final_report.to_dict(orient='records'),
        }
        return JsonResponse(result, safe=False)
    
    def get_roster_by_customer(self,request,decoded_payload,output='json'):
        # test = request.GET.get('test', '')
        user_id = request.GET.get('userid',decoded_payload.get('userid'))
        bulan_raw = request.GET.get('bulan', '')
        tahun_raw = request.GET.get('tahun', '')
        if bulan_raw and tahun_raw:
            bulan = int(bulan_raw)
            tahun = int(tahun_raw)
            
            # 2. Tentukan tanggal mulai
            start_date = f"{tahun}-{bulan:02d}-01"
            
            # 3. Ambil hari terakhir pada bulan tersebut menggunakan calendar.monthrange
            # monthrange mengembalikan tuple: (hari_pertama_dalam_minggu, jumlah_hari_dalam_bulan)
            _, jumlah_hari = calendar.monthrange(tahun, bulan)
            
            # 4. Tentukan tanggal akhir
            end_date = f"{tahun}-{bulan:02d}-{jumlah_hari}"
            
            # Jika Anda membutuhkan end_date dalam format Pandas Timestamp:
            # end_date = pd.to_datetime(f"{tahun}-{bulan:02d}-{jumlah_hari}")

            print(f"Start: {start_date}, End: {end_date}")
        else:
            print("Parameter bulan atau tahun tidak valid/kosong")
            start_date=datetime.today().strftime('%Y-%m-%d')
            end_date=datetime.today().strftime('%Y-%m-%d')
        query_handle_toko = """
            SELECT DISTINCT c.fn_cusid
            FROM user_tb u
            JOIN employees_tb e ON u.fn_userid = e.fn_userid
            JOIN t_aksesusercust t ON u.fn_userid = t.fn_userid
            JOIN customer_tb c ON t.fn_cusid = c.fn_cusid
            JOIN t_aksesusercust ts ON c.fn_cusid = ts.fn_cusid
            WHERE u.fn_userlevel IN (1, 8, 13) AND e.fn_iddepart IN (8, 20)
            AND e.fc_hold = 'F' AND c.fc_hold = 'F' AND ts.fn_userid = %s
            ORDER BY c.fn_cusid;
        """
        raw_data = DatabaseConnector.execute_raw_sql(
            query=query_handle_toko, params=[user_id], using='payroll'
            )
        df_toko_by_user=pd.DataFrame(raw_data)
        list_toko=list_to_sql_in(df_toko_by_user,'fn_cusid')
        range_tgl_clause = "AND DATE(fd_effective_from) <= DATE(%s) and( DATE(%s) <= DATE(fd_effective_until) OR fd_effective_until IS NULL)"
        query_roster = f"""
            select fn_shift_version_id, fn_cusid, fv_shift_code, fv_shift_name, 
            fd_start_time, fd_end_time, a.fn_shift_id,fv_week_type,fd_effective_from,fd_effective_until
            FROM customer_shifts a
            left join shifts_tb b on a.fn_shift_id=b.fn_shift_id
            left join shift_segments c on a.fn_shift_id=c.fn_shift_id
            WHERE a.fn_cusid IN ({list_toko}) {range_tgl_clause} or a.fn_shift_version_id = 0

        """
        query_params = [end_date, start_date]
        raw_data = DatabaseConnector.execute_raw_sql(
            query=query_roster,
            params=query_params,
            using='payroll',
            p_queri=False
        )
        # qqq=query_roster
        df_roster = pd.DataFrame(raw_data)
        if df_roster.empty:
            df_roster=pd.DataFrame(columns=['fn_cusid', 'fv_shift_name','fn_shift_version_id',
                                            'fv_shift_code', 'fd_start_time', 'fd_end_time',
                                            'fn_shift_id','fv_week_type','fd_effective_from','fd_effective_until'])
        list_shift = ['Shift Pagi', 'Shift Middle', 'Shift Siang']
        list_week_type = ['WEEKDAY', 'WEEKEND']
        # 2. Buat DataFrame awal untuk masing-masing
        df_shift = pd.DataFrame([
                    {'fv_shift_name': s, 'fv_week_type': w} for s in list_shift for w in list_week_type
                ])
        df_cross = df_toko_by_user.merge(df_shift, how='cross')
        
        # -------------------------------------------------------------
        # PROSES PEMBUATAN KALENDER LENGKAP PER USER & TOKO
        # -------------------------------------------------------------
        # 5. Gabungkan matriks kalender lengkap dengan data roster asli (LEFT JOIN)
        df_hasil = pd.merge(
            df_cross,
            df_roster,
            on=['fn_cusid', 'fv_shift_name', 'fv_week_type'],
            how='left'
        )
        df_cus = df__customers_pb_('fn_cusid,fc_code,fv_toko',where=f'and fn_cusid IN ({list_toko})')
        df_hasil = pd.merge(df_cus,df_hasil,on='fn_cusid',how='right')
        df_hasil['fn_shift_version_id']=df_hasil['fn_shift_version_id'].fillna(int(0))
        query_default = f"""
            SELECT s.fv_shift_code, s.fv_shift_name, ss.fd_start_time, ss.fd_end_time, ss.fn_shift_id
            FROM shift_segments ss
            JOIN shifts_tb s ON s.fn_shift_id = ss.fn_shift_id where fv_description like '%%Default%%'

        """
        raw_data = DatabaseConnector.execute_raw_sql(
            query=query_default,
            using='payroll',
            p_queri=False
        )
        df_default = pd.DataFrame(raw_data)
        # -------------------------------------------------------------
        # PROSES FILLING / PENGISIAN DATA NaN MENGGUNAKAN DF_DEFAULT
        # -------------------------------------------------------------
        if not df_hasil.empty and not df_default.empty:
            # print(df_hasil[['fv_shift_name','fn_shift_id']])
            # 1. Lakukan merge df_hasil dengan df_default berdasarkan 'fv_shift_name'
            # Kita beri akhiran (suffix) agar kolom dari df_default tidak langsung menimpa
            df_merge_default = pd.merge(
                df_hasil, 
                df_default, 
                on='fv_shift_name', 
                how='left', 
                suffixes=('', '_default')
            )
            
            # 2. Isi kolom yang NaN menggunakan nilai dari kolom default-nya
            df_hasil['fv_shift_code'] = df_merge_default['fv_shift_code'].combine_first(df_merge_default['fv_shift_code_default'])
            df_hasil['fd_start_time'] = df_merge_default['fd_start_time'].combine_first(df_merge_default['fd_start_time_default'])
            df_hasil['fd_end_time'] = df_merge_default['fd_end_time'].combine_first(df_merge_default['fd_end_time_default'])
            df_hasil['fn_shift_id'] = df_merge_default['fn_shift_id'].combine_first(df_merge_default['fn_shift_id_default'])
        
        df_hasil=df_hasil.fillna('-')
        result = {
            # "raw2": df_roster.to_dict(orient='records') if not df_roster.empty else [],
            "roster": df_hasil.sort_values('fc_code').to_dict(orient='records'),
            # "query": qqq,
        }

        if output=='df':
            return df_hasil.sort_values('fc_code')
        
        return JsonResponse(result, safe=False)
        
    def get_roster_by_u(self,request,decoded_payload,output='json'):
        user_id = request.GET.get('userid',decoded_payload.get('userid'))
        bulan_raw = request.GET.get('bulan', '')
        tahun_raw = request.GET.get('tahun', '')
        if bulan_raw and tahun_raw:
            bulan = int(bulan_raw)
            tahun = int(tahun_raw)
            
            # 2. Tentukan tanggal mulai
            start_date = f"{tahun}-{bulan:02d}-01"
            
            # 3. Ambil hari terakhir pada bulan tersebut menggunakan calendar.monthrange
            # monthrange mengembalikan tuple: (hari_pertama_dalam_minggu, jumlah_hari_dalam_bulan)
            _, jumlah_hari = calendar.monthrange(tahun, bulan)
            
            # 4. Tentukan tanggal akhir
            end_date = f"{tahun}-{bulan:02d}-{jumlah_hari}"
            
            # Jika Anda membutuhkan end_date dalam format Pandas Timestamp:
            # end_date = pd.to_datetime(f"{tahun}-{bulan:02d}-{jumlah_hari}")

            print(f"Start: {start_date}, End: {end_date}")
        else:
            print("Parameter bulan atau tahun tidak valid/kosong")
            start_date=datetime.today().strftime('%Y-%m-%d')
            end_date=datetime.today().strftime('%Y-%m-%d')
        query_handle_toko = """
            SELECT DISTINCT u.fn_userid, ifnull(e.fn_cus_stay,0) fn_cusid
            FROM user_tb u
            JOIN employees_tb e ON u.fn_userid = e.fn_userid
            JOIN t_aksesusercust t ON u.fn_userid = t.fn_userid
            JOIN customer_tb c ON t.fn_cusid = c.fn_cusid
            JOIN t_aksesusercust ts ON c.fn_cusid = ts.fn_cusid
            WHERE u.fn_userlevel IN (1, 8, 13) AND e.fn_iddepart IN (8, 20)
            AND e.fc_hold = 'F' AND u.fc_hold = 'F' AND c.fc_hold = 'F' AND ts.fn_userid = %s
            ORDER BY c.fn_cusid;
        """
        raw_data = DatabaseConnector.execute_raw_sql(
            query=query_handle_toko, params=[user_id], using='payroll'
            )
        df_toko_by_user=pd.DataFrame(raw_data)
        list_toko=list_to_sql_in(df_toko_by_user,'fn_cusid')
        range_tgl_clause = "AND DATE(fd_effective_from) <= DATE(%s) and( DATE(%s) <= DATE(fd_effective_until) OR fd_effective_until IS NULL)"
        query_roster = f"""
            SELECT fn_userid, ifnull(a.fn_cusid,0) fn_cusid, ifnull(fv_shift_name,fc_status) as fv_shift_name,
            fc_status,fd_schedule_date,c.fn_shift_id,fv_week_type
            FROM absensi_roster a
            left join customer_shifts b on a.fn_shift_version_id=b.fn_shift_version_id
            left join shifts_tb c on a.fn_shift_id=c.fn_shift_id or b.fn_shift_id=c.fn_shift_id
            WHERE a.fn_cusid IN ({list_toko}) {range_tgl_clause} or a.fn_shift_version_id = 0

        """
        query_params = [start_date, start_date]
        raw_data = DatabaseConnector.execute_raw_sql(
            query=query_roster,
            params=query_params,
            using='payroll',
            p_queri=False
        )
        df_roster = pd.DataFrame(raw_data)
        result = {
            # "raw2": df_roster.to_dict(orient='records') if not df_roster.empty else [],
            "roster": df_roster.sort_values('fc_code').to_dict(orient='records'),
            # "query": qqq,
        }
        if output=='df':
            return df_roster.sort_values('fc_code')
        
        return JsonResponse(result, safe=False)
    
    def get_sp_cus_roster(self,request,decoded_payload):
        user_id = request.GET.get('userid',decoded_payload.get('userid'))
        query_handle_toko = """
            SELECT DISTINCT c.fn_cusid
            FROM user_tb u
            JOIN employees_tb e ON u.fn_userid = e.fn_userid
            JOIN t_aksesusercust t ON u.fn_userid = t.fn_userid
            JOIN customer_tb c ON t.fn_cusid = c.fn_cusid
            JOIN t_aksesusercust ts ON c.fn_cusid = ts.fn_cusid
            WHERE u.fn_userlevel IN (1, 8, 13) AND e.fn_iddepart IN (8, 20)
            AND e.fc_hold = 'F' AND c.fc_hold = 'F' AND ts.fn_userid = %s
            ORDER BY c.fn_cusid;
        """
        raw_data = DatabaseConnector.execute_raw_sql(
            query=query_handle_toko, params=[user_id], using='payroll'
            )
        df_toko_by_user=pd.DataFrame(raw_data)
        list_toko=list_to_sql_in(df_toko_by_user,'fn_cusid')
        query_cus_sp = f"""
            SELECT fn_cusid,s.fv_shift_code, s.fv_shift_name, ss.fd_start_time, ss.fd_end_time, s.fn_shift_id,fd_effective_from,fd_effective_until
            FROM customer_shifts_special css
            JOIN shifts_tb s ON s.fn_shift_id = css.fn_shift_id
            join shift_segments ss ON ss.fn_shift_id = css.fn_shift_id
            where fn_cusid in ({list_toko})
        """
        raw_data = DatabaseConnector.execute_raw_sql(
            query=query_cus_sp, using='payroll'
            )
        df=pd.DataFrame(raw_data)
        if df.empty:
            return JsonResponse({"raw2": [], "roster": []}, safe=False)
        df_cus = df__customers_pb_('fn_cusid,fc_code,fv_toko',where=f'and fn_cusid IN ({list_toko})')
        df_hasil = pd.merge(df_cus,df,on='fn_cusid',how='right')
        
        result = {
            # "raw2": df.to_dict(orient='records'),
            "shift": df_hasil.to_dict(orient='records'),
        }
        return JsonResponse(result, safe=False)

    def get_shifts_code(self):
        query_handle_toko = """
            SELECT s.fv_shift_code, s.fv_shift_name, ss.fd_start_time, ss.fd_end_time, s.fn_shift_id
            FROM shift_segments ss
            JOIN shifts_tb s ON s.fn_shift_id = ss.fn_shift_id
        """
        raw_data = DatabaseConnector.execute_raw_sql(
            query=query_handle_toko, using='payroll'
            )
        df=pd.DataFrame(raw_data)
        if df.empty:
            return JsonResponse({"raw2": [], "roster": []}, safe=False)

        df_nested = (
            df.groupby(['fn_shift_id','fv_shift_code', 'fv_shift_name'])
            # MENGGUNAKAN orient='records' agar menghasilkan list of dict untuk setiap segmen
            .apply(lambda x: x[['fd_start_time', 'fd_end_time']].to_dict(orient='records'))
            .reset_index(name='segments') # Mengubah nama kolom menjadi 'segments' agar lebih deskriptif
        )
        
        result = {
            # "raw2": df.to_dict(orient='records'),
            "shift": df_nested.to_dict(orient='records'),
        }
        return JsonResponse(result, safe=False)
    
    def post_register_shift(self, request,decoded_payload, *args, **kwargs):
        """
        Endpoint untuk mendaftarkan/update shift master beserta segment jamnya.
        Frontend hanya kirim: fv_shift_name, fv_description, segments, fn_userid.

        fv_shift_code sekarang AUTO-GENERATED dengan format prefix + 3 digit urut
        (P001, P002, ... / M001, M002, ... / S001, ... / Z001, ...), prefix
        ditentukan dari keyword di fv_shift_name (Pagi/Middle/Siang/Split).

        Karena kode tidak lagi unik per kategori, fv_shift_name dipakai sebagai
        kunci identitas utama untuk membedakan INSERT (shift baru) vs UPDATE
        (shift yang sudah pernah didaftarkan dengan nama yang sama).
        """
        try:
            payload = json.loads(request.body.decode('utf-8'))
        except json.JSONDecodeError:
            return JsonResponse({'status': False, 'message': 'Format JSON tidak valid'}, status=400)

        data = payload.get('data')
        db_target = 'payroll'

        shift_id = data.get('fn_shift_id')  # opsional
        shift_code = data.get('fv_shift_code')
        shift_name = data.get('fv_shift_name')
        description = data.get('fv_description')
        fc_hold = data.get('fc_hold')
        segments = data.get('segments')
        user_id = decoded_payload.get('userid')
        current_time = datetime.now()

        if not shift_name:
            return JsonResponse({'status': False, 'message': 'fv_shift_name wajib diisi'}, status=400)

        # =========================================================================
        # STEP 0: Tentukan prefix & is_split dari nama (kode lengkap belum ditentukan di sini)
        # =========================================================================
        prefix, is_split = self._resolve_shift_prefix_and_split(shift_name)
        if not prefix:
            return JsonResponse({
                'status': False,
                'message': f'fv_shift_name "{shift_name}" tidak dikenali. Harus mengandung salah satu: Pagi, Middle, Siang, atau Split'
            }, status=400)

        # =========================================================================
        # STEP 1: Cari existing record (by fn_shift_id, fallback ke fv_shift_name)
        # =========================================================================
        existing = None
        if shift_id:
            query_by_id = "SELECT * FROM shifts_tb WHERE fn_shift_id = %s"
            existing = DatabaseConnector.execute_raw_sql(query=query_by_id, params=[shift_id], using=db_target, fetch_one=True)

        if not existing:
            query_by_name = "SELECT * FROM shifts_tb WHERE fv_shift_code = %s"
            existing = DatabaseConnector.execute_raw_sql(query=query_by_name, params=[shift_name], using=db_target, fetch_one=True)
            if existing:
                shift_id = existing['fn_shift_id']
        # =========================================================================
        # STEP 2A: TIDAK DITEMUKAN -> INSERT BARU dengan kode auto-generate
        # =========================================================================
        if not existing:
            if not segments:
                return JsonResponse({'status': False, 'message': 'segments wajib diisi untuk insert baru'}, status=400)

            if is_split == 'T' and len(segments) < 2:
                return JsonResponse({'status': False, 'message': 'Shift split wajib memiliki minimal 2 segment'}, status=400)

            jam_shift = self._check_existing_shift_by_time(segments, db_target)
            if jam_shift:
                return JsonResponse({
                    'status': False,
                    'action': 'TIME_ALREADY_REGISTERED',
                    'message': (
                        f"Jam {jam_shift['fd_start_time']} - {jam_shift['fd_end_time']} "
                        f"sudah didaftarkan dengan shift_code {jam_shift['fv_shift_code']} "
                        f"({jam_shift['fv_shift_name']})"
                    ),
                    'fv_shift_code': jam_shift['fv_shift_code'],
                    'fv_shift_name': jam_shift['fv_shift_name']
                }, status=409)
            
            shift_code = self._generate_next_shift_code(prefix, db_target)

            query_insert_shift = """
                INSERT INTO shifts_tb (fv_shift_code, fv_shift_name, fc_is_split, fv_description, fc_hold, fd_created_at,fn_userid_in)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            new_shift_id = DatabaseConnector.execute_raw_sql(
                query=query_insert_shift,
                params=[shift_code, shift_name, is_split, description, fc_hold or 'F', current_time,user_id],
                using=db_target, return_last_id=True
            )

            total_duration = self._replace_shift_segments(new_shift_id, segments, db_target, current_time)

            return JsonResponse({
                'status': True,
                'action': 'INSERTED',
                'message': f'Shift {shift_code} berhasil didaftarkan',
                'fn_shift_id': new_shift_id,
                'fv_shift_code': shift_code,
                'fc_is_split': is_split,
                'total_duration_minutes': total_duration
            }, status=201)

        # =========================================================================
        # STEP 2B: DITEMUKAN -> Bandingkan, lalu UPDATE kalau ada perubahan
        # Catatan: fv_shift_code TIDAK diubah saat update, kode bersifat permanen
        # sejak pertama kali di-generate.
        # =========================================================================
        incoming_fields = {
            'fv_shift_name': shift_name,
            'fc_is_split': is_split,
            'fv_description': description,
            'fc_hold': fc_hold,
        }

        changed_fields = {}
        for field, new_value in incoming_fields.items():
            if new_value is not None and str(existing.get(field)) != str(new_value):
                changed_fields[field] = new_value

        # Cek perubahan segments (kalau dikirim)
        segments_changed = False
        if segments is not None:
            query_existing_segments = """
                SELECT fn_segment_order, fd_start_time, fd_end_time 
                FROM shift_segments WHERE fn_shift_id = %s ORDER BY fn_segment_order
            """
            existing_segments = DatabaseConnector.execute_raw_sql(query=query_existing_segments, params=[shift_id], using=db_target) or []

            if len(existing_segments) != len(segments):
                segments_changed = True
            else:
                for old_seg, new_seg in zip(existing_segments, segments):
                    if (str(old_seg['fd_start_time']) != str(new_seg.get('fd_start_time')) or
                            str(old_seg['fd_end_time']) != str(new_seg.get('fd_end_time'))):
                        segments_changed = True
                        break

        # TIDAK ADA PERUBAHAN SAMA SEKALI -> skip
        if not changed_fields and not segments_changed:
            return JsonResponse({
                'status': True,
                'action': 'NO_CHANGE',
                'message': f'Tidak ada perubahan untuk fn_shift_id={shift_id}',
                'fn_shift_id': shift_id,
                'fv_shift_code': existing.get('fv_shift_code')
            }, status=200)

        # ADA PERUBAHAN field shifts_tb -> UPDATE
        if changed_fields:
            set_clauses = [f"{field} = %s" for field in changed_fields]
            params = list(changed_fields.values())
            set_clauses.append("fd_updated_at = %s")
            params.append(current_time)
            params.append(shift_id)

            query_update_shift = f"UPDATE shifts_tb SET {', '.join(set_clauses)} WHERE fn_shift_id = %s"
            DatabaseConnector.execute_raw_sql(query=query_update_shift, params=params, using=db_target)

        # ADA PERUBAHAN segments -> replace (delete + insert ulang)
        total_duration = None
        if segments_changed:
            final_is_split = changed_fields.get('fc_is_split', existing.get('fc_is_split'))
            if final_is_split == 'T' and len(segments) < 2:
                return JsonResponse({'status': False, 'message': 'Shift split wajib memiliki minimal 2 segment'}, status=400)
            if len(segments) == 0:
                return JsonResponse({'status': False, 'message': 'segments tidak boleh kosong'}, status=400)

            total_duration = self._replace_shift_segments(shift_id, segments, db_target, current_time)

        return JsonResponse({
            'status': True,
            'action': 'UPDATED',
            'message': f'Shift fn_shift_id={shift_id} berhasil diupdate',
            'fn_shift_id': shift_id,
            'fv_shift_code': existing.get('fv_shift_code'),
            'changed_fields': list(changed_fields.keys()),
            'segments_changed': segments_changed,
            'total_duration_minutes': total_duration
        }, status=200)

    def post_upsert_customer_shift(self, request,decoded_payload, *args, **kwargs):
        """
        Endpoint untuk memproses data assignment shift customer/toko dengan SCD Type 2 Raw SQL.
        Versi optimasi: tidak lagi menyimpan jam operasional, karena jam kerja sudah didefinisikan
        di tabel master `shifts_tb` (fn_shift_id). Tabel `customer_shifts` hanya melacak periode
        efektif (fd_effective_from / fd_effective_until) dan status hold (fc_hold) per
        (fn_cusid, fn_shift_id).

        Catatan desain:
        - "current record" ditentukan dari fd_effective_until IS NULL (bukan flag fc_is_current,
        karena kolom tersebut sudah tidak ada di tabel baru).
        - Satu customer bisa punya lebih dari satu fn_shift_id aktif secara bersamaan
        (misal Pagi & Siang berjalan paralel), masing-masing punya timeline sendiri.
        """
        db_target='payroll'
        try:
            data = json.loads(request.body.decode('utf-8'))
        except json.JSONDecodeError:
            return JsonResponse({'status': False, 'message': 'Format JSON tidak valid'}, status=400)

        shifts_list = data.get('shifts', [])
        
        if not shifts_list:
            return JsonResponse({'status': False, 'message': 'Tidak ada data shifts yang dikirim'}, status=400)

        # df = pd.DataFrame(shifts_list)
        # print(df)
        current_time = datetime.now()
        log_actions = []
        
        # =========================================================================
        # PROSES: CUSTOMER SHIFTS (SCD TYPE 2) - berbasis fn_shift_id (master shifts_tb)
        # =========================================================================
        for shift in shifts_list:
            cusid = shift.get('fn_cusid')
            shift_id = shift.get('fn_shift_id')
            fc_hold = shift.get('fc_hold', 'F')
            tgl_efektif = shift.get('fd_effective_from')
            user_id = decoded_payload.get('userid')
            version_id = shift.get('fn_shift_version_id')
            week_type= shift.get('fv_week_type')
            shif_name=shift.get('fv_shift_name')
            if version_id:
                # 1. Validasi shift_id baru tetap harus aktif di master
                # (Gunakan query_check_master yang sudah Anda buat)
                
                # 2. Eksekusi UPDATE langsung untuk koreksi data salah setup
                query_correction = """
                    UPDATE customer_shifts
                    SET fn_shiftd_id = %s,
                        fd_updated_at = %s,
                        fn_userid_up = %s,
                        fv_week_type = %s
                    WHERE fn_shift_version_id = %s
                """
                DatabaseConnector.execute_raw_sql(
                    query=query_correction,
                    params=[shift_id, current_time, user_id, version_id,week_type],
                    using=db_target
                )
                log_actions.append(f"VERSION_ID_{version_id}: DATA_CORRECTED_DIRECTLY")
                continue # Lanjut ke item berikutnya, skip baris SCD Type 2 di bawahnya

            if not all([cusid, shift_id, tgl_efektif]):
                log_actions.append("SHIFT_SKIPPED: data tidak lengkap (fn_cusid/fn_shift_id/fd_effective_from wajib)")
                continue

            # (Opsional) Validasi shift_id memang ada & aktif di master
            query_check_master = """
                SELECT fn_shift_id FROM shifts_tb WHERE fn_shift_id = %s AND fc_hold = 'F'
            """
            master_exists = DatabaseConnector.execute_raw_sql(
                query=query_check_master, params=[shift_id], using=db_target, fetch_one=True
            )
            if not master_exists:
                log_actions.append(f"SHIFT_ID_{shift_id}: SKIPPED_INVALID_OR_HOLD_MASTER")
                continue

            # Cari record yang "tertabrak" oleh tanggal efektif baru, untuk customer+shift yang sama
            query_tertabrak_sf = """
                SELECT fn_shift_version_id, c_shift.fc_hold, fd_effective_from, fd_effective_until, c_shift.fn_shift_id
                FROM customer_shifts c_shift
                inner join shifts_tb c_seg on c_seg.fn_shift_id = c_shift.fn_shift_id 
                WHERE fn_cusid = %s AND fv_shift_name = %s
                AND (fd_effective_from <= %s
                or (fd_effective_until >= %s OR fd_effective_until IS NULL))
                AND fv_week_type = %s
                ORDER BY fd_effective_from DESC LIMIT 1
            """
            record_tertabrak_sf = DatabaseConnector.execute_raw_sql(
                query=query_tertabrak_sf,
                params=[cusid, shif_name, tgl_efektif, tgl_efektif, week_type],
                using=db_target, fetch_one=True
            )

            print(record_tertabrak_sf)
            if record_tertabrak_sf:
                # Tidak ada perubahan nilai (fc_hold sama) -> abaikan, tidak perlu versi baru
                if str(record_tertabrak_sf['fn_shift_id']) == str(shift_id):
                    log_actions.append(f"SHIFT_ID_{shift_id}: BACKDATE_NO_CHANGE_IGNORED")
                    continue

                tgl_awal_asli_sf = record_tertabrak_sf['fd_effective_from']
                if isinstance(tgl_efektif, str):
                    tgl_efektif = datetime.strptime(tgl_efektif, '%Y-%m-%d').date()
                else:
                    tgl_efektif = tgl_efektif
                if tgl_awal_asli_sf < tgl_efektif:
                    
                    # Tutup record lama, pecah timeline di tanggal efektif baru
                    tgl_awal_asli_sf = record_tertabrak_sf['fd_effective_until']
                    query_update_sf = """
                        UPDATE customer_shifts
                        SET fd_effective_until = %s, fd_updated_at = %s, fn_userid_up = %s, 
                            fv_week_type = %s
                        WHERE fn_shift_version_id = %s
                    """
                    DatabaseConnector.execute_raw_sql(
                        query=query_update_sf,
                        params=[tgl_efektif, current_time, user_id,week_type, record_tertabrak_sf['fn_shift_version_id']],
                        using=db_target)

                # Insert versi baru, lanjutkan effective_until lama (kalau ada) agar timeline tidak bolong
                query_insert_sf = """
                    INSERT INTO customer_shifts (
                        fn_cusid, fn_shift_id, fc_hold,
                        fd_effective_from, fd_effective_until,
                        fd_created_at, fn_userid_in, 
                        fv_week_type 
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """
                DatabaseConnector.execute_raw_sql(
                    query=query_insert_sf,
                    params=[cusid, shift_id, fc_hold or "F", tgl_efektif, tgl_awal_asli_sf, current_time, user_id, week_type],
                    using=db_target
                )
                log_actions.append(f"SHIFT_ID_{shift_id}: TIMELINE_SPLITTED")

            else:
                # Tidak ada record yang tertabrak -> cek apakah ada record di masa depan
                # untuk menyambung fd_effective_until agar timeline tetap rapi
                query_depan_sf = """
                    SELECT fn_shift_version_id, fd_effective_from FROM customer_shifts
                    WHERE fn_cusid = %s AND fn_shift_id = %s AND fd_effective_from > %s
                    ORDER BY fd_effective_from ASC LIMIT 1
                """
                record_depan_sf = DatabaseConnector.execute_raw_sql(
                    query=query_depan_sf, params=[cusid, shift_id, tgl_efektif],
                    using=db_target, fetch_one=True
                )

                fd_effective_until_val = record_depan_sf['fd_effective_from'] if record_depan_sf else None

                query_insert_new_sf = """
                    INSERT INTO customer_shifts (
                        fn_cusid, fn_shift_id, fc_hold,
                        fd_effective_from, fd_effective_until,
                        fd_created_at, fn_userid_in, 
                        fv_week_type 
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """
                DatabaseConnector.execute_raw_sql(
                    query=query_insert_new_sf,
                    params=[cusid, shift_id, fc_hold, tgl_efektif, fd_effective_until_val, current_time, user_id,week_type],
                    using=db_target
                )
                log_actions.append(f"SHIFT_ID_{shift_id}: INSERT_NEW_TIMELINE")

        return JsonResponse({'status': True, 'details': log_actions}, status=200)
    
    @transaction.atomic(using='payroll') # Menjamin integritas data (ACID) menggunakan transaksi database
    def post_upsert_customer_shift_special(self, request, decoded_payload, *args, **kwargs):
        """
        Endpoint untuk memproses data assignment shift customer/toko pada TANGGAL SPESIAL.
        Merujuk ke tabel: `customer_shifts_special`
        """
        db_target = 'payroll'
        try:
            data = json.loads(request.body.decode('utf-8'))
        except json.JSONDecodeError:
            return JsonResponse({'status': False, 'message': 'Format JSON tidak valid'}, status=400)

        shifts_list = data.get('shifts', [])
        if not shifts_list:
            return JsonResponse({'status': False, 'message': 'Tidak ada data shifts yang dikirim'}, status=400)

        current_time = datetime.now()
        log_actions = []
        user_id = decoded_payload.get('userid')
        
        for shift in shifts_list:
            cusid = shift.get('fn_cusid')
            shift_id = shift.get('fn_shift_id')
            fc_hold = shift.get('fc_hold', 'F')
            tgl_efektif = shift.get('fd_effective_from')
            tgl_sampai = shift.get('fd_effective_until')
            version_id = shift.get('fn_shift_version_id') # PK: fn_shift_version_id
            keterangan = shift.get('fv_keterangan', '')   # Kolom khusus fv_keterangan

            # ---------------------------------------------------------------------
            # KONDISI 1: KOREKSI DATA LANGSUNG (Berdasarkan fn_shift_version_id)
            # ---------------------------------------------------------------------
            if version_id:
                query_correction = """
                    UPDATE customer_shifts_special
                    SET fn_shift_id = %s,
                        fc_hold = %s,
                        fd_effective_from = %s,
                        fd_effective_until = %s,
                        fv_keterangan = %s,
                        fd_updated_at = %s,
                        fn_userid_up = %s
                    WHERE fn_shift_version_id = %s
                """
                DatabaseConnector.execute_raw_sql(
                    query=query_correction,
                    params=[shift_id, fc_hold, tgl_efektif, tgl_sampai, keterangan, current_time, user_id, version_id],
                    using=db_target
                )
                log_actions.append(f"VERSION_ID_{version_id}: DATA_CORRECTED_DIRECTLY_IN_SPECIAL")
                continue 

            # Validasi field wajib untuk data baru
            if not all([cusid, shift_id, tgl_efektif]):
                log_actions.append("SHIFT_SKIPPED: data tidak lengkap (fn_cusid/fn_shift_id/fd_effective_from wajib)")
                continue

            # Validasi master shift (tetap pastikan ID shift valid dan aktif)
            query_check_master = "SELECT fn_shift_id FROM shifts_tb WHERE fn_shift_id = %s AND fc_hold = 'F'"
            master_exists = DatabaseConnector.execute_raw_sql(
                query=query_check_master, params=[shift_id], using=db_target, fetch_one=True
            )
            if not master_exists:
                log_actions.append(f"SHIFT_ID_{shift_id}: SKIPPED_INVALID_OR_HOLD_MASTER")
                continue

            # Parsing format tanggal jika dikirim berupa string
            if isinstance(tgl_efektif, str):
                tgl_efektif = datetime.strptime(tgl_efektif, '%Y-%m-%d').date()
            if tgl_sampai and isinstance(tgl_sampai, str):
                tgl_sampai = datetime.strptime(tgl_sampai, '%Y-%m-%d').date()

            # ---------------------------------------------------------------------
            # KONDISI 2: PROSES SCD TYPE 2 (Pengecekan Timeline Ter-overlap di tabel Special)
            # ---------------------------------------------------------------------
            # Mencari record spesial sebelumnya untuk customer yang sama yang aktif di tanggal tersebut
            query_tertabrak_sf = """
                SELECT fn_shift_version_id, fc_hold, fd_effective_from, fd_effective_until, fn_shift_id
                FROM customer_shifts_special
                WHERE fn_cusid = %s AND fn_shift_id = %s
                AND (fd_effective_from <= %s AND (fd_effective_until >= %s OR fd_effective_until IS NULL))
                ORDER BY fd_effective_from DESC LIMIT 1
            """
            record_tertabrak_sf = DatabaseConnector.execute_raw_sql(
                query=query_tertabrak_sf,
                params=[cusid, shift_id, tgl_efektif, tgl_efektif], # Tambahkan shift_id di params
                using=db_target, fetch_one=True
            )

            if record_tertabrak_sf:
                # Jika data sama persis (shift, hold, keterangan), abaikan untuk menghindari redundansi baris database
                if str(record_tertabrak_sf['fn_shift_id']) == str(shift_id) and record_tertabrak_sf['fc_hold'] == fc_hold:
                    log_actions.append(f"SHIFT_ID_{shift_id}: BACKDATE_SPECIAL_NO_CHANGE_IGNORED")
                    continue

                tgl_awal_asli_sf = record_tertabrak_sf['fd_effective_from']
                tgl_akhir_asli_sf = record_tertabrak_sf['fd_effective_until']

                if tgl_awal_asli_sf < tgl_efektif:
                    # Tutup/pecah timeline record lama tepat sehari sebelum tanggal efektif baru mulai
                    query_update_sf = """
                        UPDATE customer_shifts_special
                        SET fd_effective_until = %s, 
                            fd_updated_at = %s, 
                            fn_userid_up = %s
                        WHERE fn_shift_version_id = %s
                    """
                    DatabaseConnector.execute_raw_sql(
                        query=query_update_sf,
                        params=[tgl_efektif, current_time, user_id, record_tertabrak_sf['fn_shift_version_id']],
                        using=db_target
                    )

                # Insert data shift spesial baru yang akan menimpa/menjadi acuan utama
                query_insert_sf = """
                    INSERT INTO customer_shifts_special (
                        fn_cusid, fn_shift_id, fc_hold, fd_effective_from, fd_effective_until,
                        fv_keterangan, fd_created_at, fn_userid_in
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """
                DatabaseConnector.execute_raw_sql(
                    query=query_insert_sf,
                    params=[cusid, shift_id, fc_hold, tgl_efektif, tgl_sampai or tgl_akhir_asli_sf, keterangan, current_time, user_id],
                    using=db_target
                )
                log_actions.append(f"SHIFT_ID_{shift_id}: TIMELINE_SPLITTED_IN_CUSTOMER_SHIFTS_SPECIAL")

            else:
                # ---------------------------------------------------------------------
                # KONDISI 3: TIDAK ADA TABRAKAN (Mencari jika ada jadwal special di masa depan)
                # ---------------------------------------------------------------------
                query_depan_sf = """
                    SELECT fd_effective_from FROM customer_shifts_special
                    WHERE fn_cusid = %s AND fd_effective_from > %s
                    ORDER BY fd_effective_from ASC LIMIT 1
                """
                record_depan_sf = DatabaseConnector.execute_raw_sql(
                    query=query_depan_sf, params=[cusid, tgl_efektif],
                    using=db_target, fetch_one=True
                )

                # Jika di masa depan ada jadwal special lain, kunci batasnya disitu, jika tidak gunakan tgl_sampai dari inputan
                fd_effective_until_val = record_depan_sf['fd_effective_from'] if record_depan_sf else tgl_sampai

                query_insert_new_sf = """
                    INSERT INTO customer_shifts_special (
                        fn_cusid, fn_shift_id, fc_hold, fd_effective_from, fd_effective_until,
                        fv_keterangan, fd_created_at, fn_userid_in
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """
                DatabaseConnector.execute_raw_sql(
                    query=query_insert_new_sf,
                    params=[cusid, shift_id, fc_hold, tgl_efektif, fd_effective_until_val, keterangan, current_time, user_id],
                    using=db_target
                )
                log_actions.append(f"SHIFT_ID_{shift_id}: INSERT_NEW_TIMELINE_IN_CUSTOMER_SHIFTS_SPECIAL")

        return JsonResponse({'status': True, 'details': log_actions}, status=200)

    def post_roster_insert(self, request, decoded_payload, *args, **kwargs):
        
        """
        Endpoint optimal menginput absensi_roster secara massal (Pandas-First Approach).

        fn_shift_version_id & fn_shift_id dicari otomatis berdasarkan fv_shift_name,
        tanggal, dan SCD Type 2 di tabel customer_shifts.

        Aturan default (sesuai requirement baru):
        - Shift regular & versi ditemukan (cocok rentang tanggal)
                -> fn_shift_version_id = versi asli, fn_shift_id = id asli dari DB
        - Shift regular dikenal (nama+customer cocok) tapi TIDAK ada versi yang
            valid untuk tanggal tsb
                -> fn_shift_version_id = 0 (default), fn_shift_id = TETAP diambil dari DB
                (bukan hanya 0 / bukan dari input client)
        - Shift sama sekali tidak dikenal (nama tidak match apapun di DB)
                -> baris ditolak, masuk ke `errors`
        - Off / Cuti / Sakit (non kerja)
                -> fn_shift_version_id = 0 DAN fn_shift_id = 0 (eksplisit, abaikan input client)
        """

        def format_timedelta_to_string(series):
            """
            Mengonversi Pandas Series (berisi string, datetime.time, datetime64, atau NaT) 
            menjadi format string HH:MM:SS secara vectorized dan aman.
            """
            if series.empty:
                return series

            # 1. HANDLE CASE BARU: Jika tipe data series adalah Datetime resmi Pandas (datetime64)
            if pd.api.types.is_datetime64_any_dtype(series):
                # Ekstrak komponen jamnya saja dan ubah menjadi string 'HH:MM:SS'
                series = pd.to_datetime(series).dt.strftime('%H:%M:%S')

            # 2. Ambil sample nilai pertama yang bukan null untuk pengecekan tipe objek Python murni
            sample_val = series.dropna().iloc[0] if not series.dropna().empty else None
            
            # 3. Jika berisi objek datetime.time (bawaan DB), ubah dulu menjadi format string 'HH:MM:SS'
            if isinstance(sample_val, time):
                series = series.apply(lambda x: x.strftime('%H:%M:%S') if isinstance(x, time) else x)
            elif isinstance(sample_val, datetime):
                # Cadangan jika ada objek datetime murni Python non-pandas lolos
                series = series.apply(lambda x: x.strftime('%H:%M:%S') if isinstance(x, datetime) else x)

            # 4. Sekarang mutlak aman untuk diubah ke Timedelta oleh Pandas
            td_series = pd.to_timedelta(series, errors='coerce').fillna(pd.Timedelta(seconds=0))
            
            # 5. Ekstrak komponen total detik untuk memformat string HH:MM:SS secara vectorized
            total_seconds = td_series.dt.total_seconds().astype(int)
            hours = total_seconds // 3600
            minutes = (total_seconds % 3600) // 60
            seconds = total_seconds % 60
            
            return (hours.astype(str).str.zfill(2) + ':' +
                    minutes.astype(str).str.zfill(2) + ':' +
                    seconds.astype(str).str.zfill(2))

        try:
            data = json.loads(request.body.decode('utf-8'))
        except json.JSONDecodeError:
            return JsonResponse({'status': False, 'message': 'Format JSON tidak valid'}, status=400)

        db_target = 'payroll'
        roster_list = data.get('roster', [])

        if not roster_list:
            return JsonResponse({'status': False, 'message': 'Tidak ada data roster yang dikirim'}, status=400)
        
        current_time = datetime.now()
        errors = []

        # ==========================================
        # STEP 1: LOAD INPUT KE DATAFRAME UTAMA
        # ==========================================
        df_roster = pd.DataFrame(roster_list)
        df_roster['fn_requested_userid'] = decoded_payload.get('userid')

        required_cols = ['fn_userid', 'fn_cusid', 'fd_schedule_date', 'fn_requested_userid', 'fv_shift_name']
        missing_cols = [col for col in required_cols if col not in df_roster.columns]
        if missing_cols:
            return JsonResponse({'status': False, 'message': f'Kolom wajib tidak ditemukan: {missing_cols}'}, status=400)

        df_roster = df_roster.dropna(subset=required_cols).copy()
        if df_roster.empty:
            return JsonResponse({'status': False, 'message': 'Semua data baris tidak lengkap'}, status=400)

        # Parsing Tanggal & Karakteristik Hari
        df_roster['date_obj'] = pd.to_datetime(df_roster['fd_schedule_date'], errors='coerce').dt.date
        
        # print(df_roster['fd_schedule_date'])
        df_roster['fv_week_type'] = np.where(
            pd.to_datetime(df_roster['fd_schedule_date']).dt.weekday >= 5, 'WEEKEND', 'WEEKDAY'
        )
        # print(df_roster['fv_week_type'])
        # print(df_roster['fn_shift_id'])
        df_roster['idx_row'] = df_roster.index

        # Pisahkan baris non-kerja (Cuti, Sakit, Off) lebih awal -> query DB hanya
        # untuk shift yang benar-benar perlu dicari (optimasi: data yang difetch lebih sedikit)
        non_kerja_names = ['cuti', 'sakit', 'off']
        df_roster['is_non_kerja'] = df_roster['fv_shift_name'].str.lower().isin(non_kerja_names)

        df_regular = df_roster[~df_roster['is_non_kerja']].copy()
        df_non_kerja = df_roster[df_roster['is_non_kerja']].copy()

        # ==========================================
        # STEP 2: BULK FETCH DATA DB & CONVERT TO DF
        # (hanya dijalankan kalau ada baris regular yang butuh shift)
        # ==========================================
        df_shifts = pd.DataFrame()

        if not df_regular.empty:
            customer_ids = df_regular['fn_cusid'].dropna().unique().tolist()
            shift_names = df_regular['fv_shift_name'].dropna().unique().tolist()
            start_date = df_regular['date_obj'].dropna().min().strftime('%Y-%m-%d')
            end_date = df_regular['date_obj'].dropna().max().strftime('%Y-%m-%d')

            format_customers = ','.join(['%s'] * len(customer_ids))
            format_shifts = ','.join(['%s'] * len(shift_names))

            # FIX: filter berdasarkan fv_shift_name (bukan fn_shift_id) karena shift_names
            # berisi NAMA shift ("Pagi", "Siang", dst), bukan ID. Filter lama membandingkan
            # fn_shift_id dengan string nama -> hampir selalu kosong -> memicu fallback
            # query "panic" yang menyamaratakan fn_shift_version_id = 0 untuk SEMUA baris,
            # termasuk customer/tanggal yang tidak relevan. Itu dihapus di versi ini.
            #
            # Join ke shift_segments juga dihapus karena tidak ada kolomnya yang dipakai,
            # dan join itu bisa menggandakan baris versi shift kalau 1 shift punya banyak
            # segment.
            query_shifts = f"""
                SELECT fn_shift_version_id, a.fn_shift_id, b.fv_shift_name, a.fn_cusid,
                    a.fd_effective_from, a.fd_effective_until, a.fv_week_type
                FROM customer_shifts a
                LEFT JOIN shifts_tb b ON a.fn_shift_id = b.fn_shift_id
                WHERE a.fn_cusid IN ({format_customers})
                AND b.fv_shift_name IN ({format_shifts})
                AND (a.fd_effective_from IS NULL OR DATE(a.fd_effective_from) <= DATE('{end_date}'))
                AND (a.fd_effective_until IS NULL OR DATE(a.fd_effective_until) >= DATE('{start_date}'))
            """
            raw_shifts = DatabaseConnector.execute_raw_sql(
                query=query_shifts, params=customer_ids + shift_names, using=db_target
            ) or []
            df_shifts = pd.DataFrame(raw_shifts)
            
            if not df_shifts.empty:
                # FIX: v_to_dt sebelumnya dihitung dari fd_effective_from (typo), seharusnya
                # dari fd_effective_until. Akibatnya rentang tanggal SCD Type 2 tidak pernah
                # valid secara benar.
                df_shifts['v_from_dt'] = pd.to_datetime(df_shifts['fd_effective_from']).dt.date
                df_shifts['v_to_dt'] = pd.to_datetime(df_shifts['fd_effective_until']).dt.date
                df_shifts['v_from_dt'] = df_shifts['v_from_dt'].fillna(pd.Timestamp('1970-01-01').date())
                df_shifts['v_to_dt'] = df_shifts['v_to_dt'].fillna(pd.Timestamp('2099-12-31').date())

        # ==========================================
        # STEP 3: DATA ANALYSIS & VALIDATION MATRIX
        # ==========================================

        # --- 3.A MATCHING SHIFT VERSION + DEFAULT fn_shift_id (Hanya untuk Regular Shift) ---
        if not df_regular.empty:
            df_regular['fc_status'] = 'pending'  # Status default untuk regular shift
            df_regular['fd_start_time'] = pd.NaT
            df_regular['fd_end_time'] = pd.NaT
            # --- 3.A.1 Match customer-specific shift version (SCD Type 2) ---
            df_regular['fn_shift_id_client'] = df_regular['fn_shift_id']
            # Pastikan kolom 'default' ada dan bernilai integer (default ke 1 jika tidak dikirim)
            if 'default' in df_regular.columns:
                df_regular['default'] = pd.to_numeric(df_regular['default'], errors='coerce').fillna(1).astype(int)
            else:
                df_regular['default'] = 1

            if not df_shifts.empty:
                # KUNCI UTAMA: Gabungkan dengan menyertakan 'fv_week_type'
                df_merge_shift = pd.merge(
                    df_regular, df_shifts, 
                    on=['fn_cusid', 'fv_shift_name', 'fv_week_type'],
                    how='left', suffixes=('', '_db')
                )
                
                # Validasi rentang tanggal SCD Type 2
                shift_mask = (
                    (df_merge_shift['date_obj'] >= df_merge_shift['v_from_dt']) &
                    (df_merge_shift['date_obj'] <= df_merge_shift['v_to_dt'])
                )
                df_valid_shifts = df_merge_shift[shift_mask].drop_duplicates(subset=['idx_row'])

                # Kembalikan hasil yang valid ke df_regular
                df_regular = pd.merge(
                    df_regular,
                    df_valid_shifts[['idx_row', 'fn_shift_version_id', 'fn_shift_id_db']], 
                    on='idx_row', how='left'
                )
                
                # -------------------------------------------------------------
                # PERBAIKAN LOGIKA UTAMA (SINKRONISASI FLAG 'default'):
                # -------------------------------------------------------------
                mask_version_found = df_regular['fn_shift_version_id'].notna()

                # Kondisi A: Versi ketemu DAN default == 1 (Pakai aturan versi resmi DB)
                # -> fn_shift_id & fn_shift_version_id keduanya diambil dari versi DB
                mask_use_db = mask_version_found & (df_regular['default'] == 1)
                df_regular.loc[mask_use_db, 'fn_shift_id'] = df_regular.loc[
                    mask_use_db, 'fn_shift_id_db'
                ]

                # Kondisi B: Frontend minta default == 0 (Shift Manual/Kustom)
                # -> fn_shift_id AMBIL DARI CLIENT (misal ID 17)
                # -> fn_shift_version_id WAJIB SET KE 0 (karena tidak terikat versi DB manapun)
                mask_use_client = df_regular['default'] == 0
                df_regular.loc[mask_use_client, 'fn_shift_id'] = (
                    df_regular.loc[mask_use_client, 'fn_shift_id_client']
                )
                df_regular.loc[mask_use_client, 'fn_shift_version_id'] = 0
            else:
                df_regular['fn_shift_version_id'] = np.nan
                # Jika default == 1, set fn_shift_id jadi NaN (biar masuk ke fallback/reject)
                # Jika default == 0, pertahankan fn_shift_id bawaan dari Frontend
                mask_default_1 = df_regular['default'] == 1
                df_regular.loc[mask_default_1, 'fn_shift_id'] = np.nan
            
            # --- 3.A.2 Fallback: gunakan shift Default global kalau belum ketemu versi valid ---
            # Trigger: customer tidak punya shift dengan nama ini sama sekali, ATAU punya
            # tapi tanggalnya di luar rentang versi manapun.
            need_default_mask = df_regular['fn_shift_version_id'].isna()
            if need_default_mask.any():
                query_default = """
                    SELECT s.fv_shift_code, s.fv_shift_name, ss.fd_start_time, ss.fd_end_time, ss.fn_shift_id
                    FROM shift_segments ss
                    JOIN shifts_tb s ON s.fn_shift_id = ss.fn_shift_id
                    WHERE s.fv_description LIKE '%%Default%%'
                """
                raw_default = (
                    DatabaseConnector.execute_raw_sql(
                        query=query_default, using=db_target, p_queri=False
                    )
                    or []
                )
                df_default = pd.DataFrame(raw_default)

                if not df_default.empty:
                    df_default_dedup = (
                        df_default.dropna(subset=['fn_shift_id'])
                        .drop_duplicates(subset=['fv_shift_name'])
                        .loc[
                            :,
                            [
                                'fv_shift_name',
                                'fn_shift_id',
                                'fd_start_time',
                                'fd_end_time',
                            ],
                        ]
                        .rename(
                            columns={
                                'fn_shift_id': 'fn_shift_id_default',
                                'fd_start_time': 'fd_start_time_default',
                                'fd_end_time': 'fd_end_time_default',
                            }
                        )
                    )

                    df_regular = pd.merge(
                        df_regular, df_default_dedup, on='fv_shift_name', how='left'
                    )

                    # -------------------------------------------------------------
                    # PENYESUAIAN DI SINI:
                    # Tambahkan syarat (df_regular['default'] == 1) pada apply_default_mask
                    # -------------------------------------------------------------
                    apply_default_mask = (
                        need_default_mask
                        & df_regular['fn_shift_id_default'].notna()
                        & (df_regular['default'] == 1)  # Hanya override jika frontend minta default == 1
                    )

                    # Untuk yang default == 1: Pakai fn_shift_id dari DB Default
                    df_regular.loc[apply_default_mask, 'fn_shift_id'] = (
                        df_regular.loc[
                            apply_default_mask, 'fn_shift_id_default'
                        ]
                    )

                    # Untuk SEMUA baris fallback (baik default 0 maupun 1):
                    # Set fn_shift_version_id = 0 dan ambil jam kerjanya jika ada
                    fallback_mask = (
                        need_default_mask
                        & df_regular['fn_shift_id_default'].notna()
                    )
                    df_regular.loc[fallback_mask, 'fn_shift_version_id'] = 0
                    df_regular.loc[fallback_mask, 'fd_start_time'] = (
                        df_regular.loc[
                            fallback_mask, 'fd_start_time_default'
                        ]
                    )
                    df_regular.loc[fallback_mask, 'fd_end_time'] = (
                        df_regular.loc[fallback_mask, 'fd_end_time_default']
                    )

                    # Jika default == 0 tapi fn_shift_id sempat bernilai NaN/kosong,
                    # kembalikan ke fn_shift_id_client bawaan frontend
                    mask_client_restore = fallback_mask & (
                        df_regular['default'] == 0
                    )
                    df_regular.loc[mask_client_restore, 'fn_shift_id'] = (
                        df_regular.loc[
                            mask_client_restore, 'fn_shift_id_client'
                        ]
                    )

                    df_regular.drop(
                        columns=[
                            'fn_shift_id_default',
                            'fd_start_time_default',
                            'fd_end_time_default',
                        ],
                        inplace=True,
                        errors='ignore',
                    )
                # --- 3.A.3 Shift yang TETAP tidak ditemukan (tidak ada di customer_shifts maupun Default) -> reject
            invalid_shift_rows = df_regular[df_regular['fn_shift_version_id'].isna()]
            for _, row in invalid_shift_rows.iterrows():
                errors.append(
                    f"Baris {row['idx_row']}: Shift ID {row['fn_shift_id_client']} ('{row['fv_shift_name']}' - {row['fv_week_type']}) "
                    f"TIDAK VALID untuk Customer {row['fn_cusid']} pada tanggal {row['fd_schedule_date']}."
                )
            # Eksekusi pembersihan
            df_regular = df_regular.dropna(subset=['fn_shift_version_id']).copy()
            df_regular.drop(columns=['fn_shift_id_client'], inplace=True, errors='ignore')

            df_regular['fd_start_time'] = format_timedelta_to_string(df_regular['fd_start_time'])
            df_regular['fd_end_time'] = format_timedelta_to_string(df_regular['fd_end_time'])
        # --- Set Default Nilai Langsung untuk Non-Kerja ---
        if not df_non_kerja.empty:
            # Eksplisit = 0 untuk Off/Cuti/Sakit, abaikan apapun yang dikirim client
            df_non_kerja['fn_shift_version_id'] = 0
            df_non_kerja['fn_shift_id'] = 0
            df_non_kerja['fc_status'] = df_non_kerja['fv_shift_name']  # "Cuti" / "Sakit" / "Off"
            df_non_kerja['fd_start_time'] = "00:00:00"
            df_non_kerja['fd_end_time'] = "00:00:00"

        # --- GABUNGKAN KEMBALI KEDUA DATAFRAME ---
        df_roster = pd.concat([df_regular, df_non_kerja], ignore_index=True)
        
        # ==========================================
        # STEP 4: PREPARE BULK INJECTION & EXECUTE
        # ==========================================
        log_actions = []
        if not df_roster.empty:
            df_roster['fv_keterangan_request'] = df_roster.get('fv_keterangan_request', '').fillna('')

            # Bersihkan nilai fn_no agar seragam (jika kosong/NaN diubah jadi None atau 0)
            if 'fn_no' in df_roster.columns:
                df_roster['fn_no'] = pd.to_numeric(df_roster['fn_no'], errors='coerce').fillna(0).astype(int)
            else:
                df_roster['fn_no'] = 0

            if 'absensi' in df_roster.columns:
                df_roster['absensi'] = df_roster['absensi'].fillna('').str.lower()
            else:
                df_roster['absensi'] = ''

            for _, row in df_roster.iterrows():
                log_actions.append(
                    f"User {row['fn_userid']} ({row['fv_shift_name']}) sukses diatur status "
                    f"{row['fc_status']} pada {row['fd_schedule_date']}"
                )

            naive_current_time = current_time.strftime('%Y-%m-%d %H:%M:%S')
            df_roster['fd_created_at'] = naive_current_time
            df_roster['fd_updated_at'] = naive_current_time

            # PROSES 1: UPSERT KE TABEL absensi_roster (JADWAL UTAMA)
            valid_payloads = df_roster[[
                'fn_userid', 'fn_cusid', 'fn_shift_version_id', 'fd_schedule_date', 'fc_status',
                'fn_requested_userid', 'fv_keterangan_request', 'fd_created_at', 'fd_updated_at',
                'fn_shift_id'
            ]].values.tolist()

            query_insert_roster = """
                INSERT INTO absensi_roster (
                    fn_userid, fn_cusid, fn_shift_version_id, fd_schedule_date,
                    fc_status, fn_requested_userid,
                    fv_keterangan_request, fd_created_at, fd_updated_at, fn_shift_id
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    fn_shift_version_id = VALUES(fn_shift_version_id),
                    fn_shift_id = VALUES(fn_shift_id),
                    fc_status = VALUES(fc_status),
                    fn_requested_userid = VALUES(fn_requested_userid),
                    fv_keterangan_request = VALUES(fv_keterangan_request),
                    fd_updated_at = VALUES(fd_updated_at);
            """

            for params in valid_payloads:
                clean_params = [int(x) if isinstance(x, (np.integer, np.int64)) else x for x in params]
                DatabaseConnector.execute_raw_sql(
                    query=query_insert_roster, params=clean_params, using=db_target, p_queri=False
                )
            # PROSES 2: SINKRONISASI KE TABEL LOG absensi_dps (RIIL ABSENSI)
            for _, row in df_roster.iterrows():
                v_fn_no = row['fn_no']
                v_absensi = row['absensi']
                v_userid = int(row['fn_userid'])
                v_tgl = row['fd_schedule_date']
                v_shift_name = row['fv_shift_name'].lower()
                blank_json_payload = json.dumps({'in': '', 'out': ''})
                # KONDISI A: fn_no TIDAK KOSONG DAN absensi BUKAN 'absen' -> LAKUKAN UPDATE
                if v_fn_no > 0 and v_absensi != 'absen':
                    query_update_dps = """
                        UPDATE absensi_dps 
                        SET fd_update = %s, 
                            stat = %s
                        WHERE fn_no = %s
                    """
                    DatabaseConnector.execute_raw_sql(
                        query=query_update_dps, 
                        params=[ naive_current_time,'d', v_fn_no], 
                        using=db_target, 
                        p_queri=False
                    )

                # KONDISI B: JIKA SHIFT ADALAH NON-KERJA (cuti/sakit/off) & BELUM ADA DI DPS -> LAKUKAN INSERT BARU
                elif v_shift_name in ['cuti', 'sakit', 'off']:
                    # 1. Cari Toko Default Aktif User
                    query_toko = """
                        SELECT fn_cusid FROM t_aksesusercust 
                        WHERE fn_userid = %s 
                        ORDER BY fn_idusercust ASC LIMIT 1
                    """
                    toko_res = DatabaseConnector.execute_raw_sql(query_toko, [v_userid], using=db_target, fetch_one=True)
                    toko_id = toko_res['fn_cusid'] if toko_res else 0
                    
                    insert_json_payload = json.dumps({'in': f'{v_shift_name}', 'out': f'{v_shift_name}'})
                    # 2. Karena status non-kerja, data koordinat dan jam dikosongkan murni string
                    query_insert_dps = """
                        INSERT INTO absensi_dps 
                        (fn_userid, fd_tanggal, fc_jenis, fc_status, fc_shift, fc_tempat, fc_jam, fc_foto, fn_toko_in, fd_create, stat)
                        VALUES (%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, 'i')
                    """
                    DatabaseConnector.execute_raw_sql(
                        query=query_insert_dps, 
                        params=[v_userid, v_tgl, v_shift_name, v_shift_name,insert_json_payload,insert_json_payload,insert_json_payload,insert_json_payload, toko_id, naive_current_time], 
                        using=db_target, 
                        p_queri=False
                    )
        if errors and not log_actions:
            return JsonResponse({'status': False, 'errors': errors}, status=400)
        return JsonResponse({'status': True, 'details': log_actions, 'ignored_or_errors': errors}, status=200)


''' VIEW `view_rekap_approval` AS
    SELECT 
        `appr`.`fn_userid` AS `userid`,
        `appr`.`fc_jenis` AS `jenis`,
        `appr`.`fd_tanggal` AS `tanggal`,
        `appr`.`fd_mulai` AS `mulai`,
        `appr`.`fd_sampai` AS `sampai`,
        `appr`.`fv_keterangan` AS `keterangan`,
        `appr`.`fn_no` AS `no`,
        `appr`.`fc_approve` AS `status`,
        (CASE
            WHEN ISNULL(`appr`.`fv_note`) THEN '-'
            ELSE `appr`.`fv_note`
        END) AS `note`,
        `e`.`fv_nameemployees` AS `spg`,
        `c`.`fn_cusid` AS `fn_cusid`,
        `c`.`fc_codecus` AS `kodetoko`,
        `c`.`fv_namecus` AS `namatoko`,
        `ts`.`fn_userid` AS `userspv`,
        `ts`.`fn_cusid` AS `tokospv`
    FROM
        (((((`gishrdpayroll`.`approval_dps_dev` `appr`
        JOIN (SELECT 
                    `gishrdpayroll`.`user_tb`.`fn_userid` AS `fn_userid`
                FROM
                    `gishrdpayroll`.`user_tb`
                WHERE
                 (`gishrdpayroll`.`user_tb`.`fn_userlevel` IN (1 , 8, 13))) `u` 
        ON ((`appr`.`fn_userid` = `u`.`fn_userid`)))
        
        JOIN (SELECT 
                `gishrdpayroll`.`employees_tb`.`fn_userid` AS `fn_userid`,
                    `gishrdpayroll`.`employees_tb`.`fv_nameemployees` AS `fv_nameemployees`
            FROM
                `gishrdpayroll`.`employees_tb`
            WHERE
                (`gishrdpayroll`.`employees_tb`.`fn_iddepart` IN (8 , 20))) `e` 
        ON ((`u`.`fn_userid` = `e`.`fn_userid`)))

        JOIN (SELECT 
                `gishrdpayroll`.`t_aksesusercust`.`fn_userid` AS `fn_userid`,
                    `gishrdpayroll`.`t_aksesusercust`.`fn_cusid` AS `fn_cusid`
            FROM
                `gishrdpayroll`.`t_aksesusercust`) `t` 
        ON ((`e`.`fn_userid` = `t`.`fn_userid`)))

        JOIN (SELECT 
                `gishrdpayroll`.`customer_tb`.`fn_cusid` AS `fn_cusid`,
                    `gishrdpayroll`.`customer_tb`.`fc_codecus` AS `fc_codecus`,
                    `gishrdpayroll`.`customer_tb`.`fv_namecus` AS `fv_namecus`
            FROM
                `gishrdpayroll`.`customer_tb`) `c` 
        ON ((`t`.`fn_cusid` = `c`.`fn_cusid`)))
        JOIN (SELECT 
                `gishrdpayroll`.`t_aksesusercust`.`fn_userid` AS `fn_userid`,
                    `gishrdpayroll`.`t_aksesusercust`.`fn_cusid` AS `fn_cusid`
            FROM
                `gishrdpayroll`.`t_aksesusercust`) `ts` 
        ON ((`c`.`fn_cusid` = `ts`.`fn_cusid`)))
    ORDER BY `appr`.`fd_mulai` DESC'''