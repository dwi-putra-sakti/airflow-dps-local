# This is an auto-generated Django model module.
# You'll have to do the following manually to clean this up:
#   * Rearrange models' order
#   * Make sure each model has one field with primary_key=True
#   * Make sure each ForeignKey and OneToOneField has `on_delete` set to the desired behavior
#   * Remove `managed = False` lines if you wish to allow Django to create, modify, and delete the table
# Feel free to rename the models, but don't rename db_table values or field names.
from django.db import models
from datetime import datetime


class UserTb(models.Model):
    fn_userid = models.AutoField(primary_key=True, db_column='fn_userid')
    fv_username = models.CharField(max_length=50, unique=True, db_column='fv_username')
    # fv_password = models.CharField(max_length=255, null=True, blank=True, db_column='fv_password')
    # fv_key = models.CharField(max_length=255, null=True, blank=True, db_column='fv_key')
    fd_expireddate = models.DateTimeField(null=True, blank=True, db_column='fd_expireddate')
    fc_hold = models.CharField(max_length=1, default='F', db_column='fc_hold')
    fc_changepassword = models.CharField(max_length=1, null=True, blank=True, db_column='fc_changepassword')
    fv_desc = models.CharField(max_length=255, null=True, blank=True, db_column='fv_desc')
    fv_password_web = models.CharField(max_length=255, null=True, blank=True, db_column='fv_password_web')
    fv_email = models.CharField(max_length=255, null=True, blank=True, db_column='fv_email')
    fn_counter = models.IntegerField(default=1, db_column='fn_counter')
    fn_userlevel = models.IntegerField(default=1, db_column='fn_userlevel')
    fn_whid = models.IntegerField(default=1, db_column='fn_whid')
    fn_userinput = models.IntegerField(null=True, blank=True, db_column='fn_userinput')
    fd_dateinput = models.DateTimeField(default=datetime.now(), db_column='fd_dateinput')
    fv_ipinput = models.CharField(max_length=100, null=True, blank=True, db_column='fv_ipinput')

    class Meta:
        db_table = 'user_tb'
        managed = False  # Set False jika tabel sudah ada di DB dan tidak ingin di-migrate ulang oleh Django

class HsUserTb(models.Model):
    fn_userid = models.IntegerField(primary_key=True, db_column='fn_userid')
    fv_username = models.CharField(max_length=50, unique=True, db_column='fv_username')
    # fv_password = models.CharField(max_length=255, null=True, blank=True, db_column='fv_password')
    # fv_key = models.CharField(max_length=255, null=True, blank=True, db_column='fv_key')
    fd_expireddate = models.DateTimeField(null=True, blank=True, db_column='fd_expireddate')
    fc_hold = models.CharField(max_length=1, default='F', db_column='fc_hold')
    fc_changepassword = models.CharField(max_length=1, null=True, blank=True, db_column='fc_changepassword')
    fv_desc = models.CharField(max_length=255, null=True, blank=True, db_column='fv_desc')
    fv_password_web = models.CharField(max_length=255, null=True, blank=True, db_column='fv_password_web')
    fv_email = models.CharField(max_length=255, null=True, blank=True, db_column='fv_email')
    fn_counter = models.IntegerField(default=1, db_column='fn_counter')
    fn_userlevel = models.IntegerField(default=1, db_column='fn_userlevel')
    fn_whid = models.IntegerField(default=1, db_column='fn_whid')
    fn_userinput = models.IntegerField(null=True, blank=True, db_column='fn_userinput')
    fd_dateinput = models.DateTimeField(default=datetime.now(), db_column='fd_dateinput')
    fv_ipinput = models.CharField(max_length=100, null=True, blank=True, db_column='fv_ipinput')

    fn_userupdate = models.IntegerField(db_column='fn_userupdate')
    fd_dateupdate = models.DateTimeField(default=datetime.now, db_column='fd_dateupdate')
    fv_ipupdate = models.CharField(max_length=100, db_column='fv_ipupdate')
    fc_action = models.CharField(max_length=1,  db_column='fc_action')
    class Meta:
        db_table = 'hs_user_tb'
        managed = False  # Set False jika tabel sudah ada di DB dan tidak ingin di-migrate ulang oleh Django

class EmployeesTb(models.Model):
    fn_idemployees = models.AutoField(primary_key=True, db_column='fn_idemployees')
    fc_codeemployees = models.CharField(max_length=16, db_column='fc_codeemployees')
    fv_alias = models.CharField(max_length=10, null=True, blank=True, db_column='fv_alias')
    fv_nameemployees = models.CharField(max_length=255, null=True, blank=True, db_column='fv_nameemployees')
    fv_npwp = models.CharField(max_length=100, null=True, blank=True, db_column='fv_npwp')
    fv_nip = models.CharField(max_length=100, null=True, blank=True, db_column='fv_nip')
    fv_nik = models.CharField(max_length=100, null=True, blank=True, db_column='fv_nik')
    fv_address = models.CharField(max_length=255, null=True, blank=True, db_column='fv_address')
    fv_email = models.CharField(max_length=255, null=True, blank=True, db_column='fv_email')
    
    fn_countryid = models.IntegerField(default=1, db_column='fn_countryid')
    fn_provinceid = models.IntegerField(default=1, db_column='fn_provinceid')
    fn_cityid = models.IntegerField(default=24, db_column='fn_cityid')
    fv_zipcode = models.CharField(max_length=100, null=True, blank=True, db_column='fv_zipcode')
    fv_phonenumber = models.CharField(max_length=100, null=True, blank=True, db_column='fv_phonenumber')
    fv_faxnumber = models.CharField(max_length=100, null=True, blank=True, db_column='fv_faxnumber')
    
    fd_datein = models.DateTimeField(null=True, blank=True, db_column='fd_datein')
    fd_dateout = models.DateTimeField(null=True, blank=True, db_column='fd_dateout')
    
    fn_userid = models.IntegerField(db_column='fn_userid')
    fn_iddepart = models.IntegerField(default=8, db_column='fn_iddepart')
    
    fc_activeowner = models.CharField(max_length=1, null=True, blank=True, db_column='fc_activeowner')
    fc_otoritation = models.CharField(max_length=1, null=True, blank=True, db_column='fc_otoritation')
    fc_hold = models.CharField(max_length=1, default='F', db_column='fc_hold')
    fv_description = models.CharField(max_length=255, null=True, blank=True, db_column='fv_description')
    
    fn_userinput = models.IntegerField(null=True, blank=True, db_column='fn_userinput')
    fd_dateinput = models.DateTimeField(default=datetime.now, db_column='fd_dateinput')
    fv_ipinput = models.CharField(max_length=100, null=True, blank=True, db_column='fv_ipinput')
    
    fn_pinid = models.IntegerField(null=True, blank=True, db_column='fn_pinid')
    fv_alamatdomisili = models.TextField(null=True, blank=True, db_column='fv_alamatdomisili')
    fv_kecamatan = models.CharField(max_length=155, null=True, blank=True, db_column='fv_kecamatan')
    fv_kelurahan = models.CharField(max_length=155, null=True, blank=True, db_column='fv_kelurahan')
    fv_rt = models.CharField(max_length=15, null=True, blank=True, db_column='fv_rt')
    fv_rw = models.CharField(max_length=15, null=True, blank=True, db_column='fv_rw')
    fv_emailpekerjaan = models.CharField(max_length=255, null=True, blank=True, db_column='fv_emailpekerjaan')
    fv_goldarah = models.CharField(max_length=3, null=True, blank=True, db_column='fv_goldarah')
    fv_ipk = models.CharField(max_length=15, null=True, blank=True, db_column='fv_ipk')
    fv_namaaAtNpwp = models.CharField(max_length=255, null=True, blank=True, db_column='fv_namaaAtNpwp')
    fv_alamatNpwp = models.TextField(null=True, blank=True, db_column='fv_alamatNpwp')
    fv_nomorrekbca = models.CharField(max_length=25, null=True, blank=True, db_column='fv_nomorrekbca')
    fv_jeniskelamin = models.CharField(max_length=2, null=True, blank=True, db_column='fv_jeniskelamin')
    fd_tgllahir = models.DateField(null=True, blank=True, db_column='fd_tgllahir')
    fv_foto = models.CharField(max_length=185, null=True, blank=True, db_column='fv_foto')
    fc_staff = models.CharField(max_length=1, default='F', db_column='fc_staff')
    
    fd_jammasuk = models.TimeField(null=True, blank=True, db_column='fd_jammasuk')
    fd_jamistrht = models.IntegerField(null=True, blank=True, db_column='fd_jamistrht')
    fd_jampulang = models.TimeField(null=True, blank=True, db_column='fd_jampulang')
    fn_idjamkerja = models.IntegerField(null=True, blank=True, db_column='fn_idjamkerja')
    
    fv_nomorKK = models.CharField(max_length=55, null=True, blank=True, db_column='fv_nomorKK')
    fn_kotalahir = models.IntegerField(null=True, blank=True, db_column='fn_kotalahir')
    fv_pendidikanakhir = models.CharField(max_length=155, null=True, blank=True, db_column='fv_pendidikanakhir')
    fv_kampus = models.CharField(max_length=155, null=True, blank=True, db_column='fv_kampus')
    fv_alasanresign = models.TextField(null=True, blank=True, db_column='fv_alasanresign')
    fc_kategoriresign = models.CharField(max_length=55,null=True, blank=True, db_column='fc_kategoriresign')
    class Meta:
        db_table = 'employees_tb'
        managed = False

class HsEmployeesTb(models.Model):
    fn_idemployees = models.AutoField(primary_key=True, db_column='fn_idemployees')
    fc_codeemployees = models.CharField(max_length=16, db_column='fc_codeemployees')
    fv_alias = models.CharField(max_length=10, db_column='fv_alias')
    fv_nameemployees = models.CharField(max_length=255,  db_column='fv_nameemployees')
    fv_npwp = models.CharField(max_length=100,  db_column='fv_npwp')
    fv_nip = models.CharField(max_length=100,  db_column='fv_nip')
    fv_nik = models.CharField(max_length=100,  db_column='fv_nik')
    fv_address = models.CharField(max_length=255,  db_column='fv_address')
    fv_email = models.CharField(max_length=255,  db_column='fv_email')
    
    fn_countryid = models.IntegerField(default=1, db_column='fn_countryid')
    fn_provinceid = models.IntegerField(default=1, db_column='fn_provinceid')
    fn_cityid = models.IntegerField(default=24, db_column='fn_cityid')
    fv_zipcode = models.CharField(max_length=100,  db_column='fv_zipcode')
    fv_phonenumber = models.CharField(max_length=100, db_column='fv_phonenumber')
    fv_faxnumber = models.CharField(max_length=100,  db_column='fv_faxnumber')
    
    fd_datein = models.DateTimeField( db_column='fd_datein')
    fd_dateout = models.DateTimeField( db_column='fd_dateout')
    
    fn_userid = models.IntegerField(db_column='fn_userid')
    fn_iddepart = models.IntegerField(default=8, db_column='fn_iddepart')
    
    fc_activeowner = models.CharField(max_length=1,  db_column='fc_activeowner')
    fc_otoritation = models.CharField(max_length=1,  db_column='fc_otoritation')
    fc_hold = models.CharField(max_length=1, default='F', db_column='fc_hold')
    fv_description = models.CharField(max_length=255,  db_column='fv_description')
    
    fn_userinput = models.IntegerField(db_column='fn_userinput')
    fd_dateinput = models.DateTimeField(default=datetime.now, db_column='fd_dateinput')
    fv_ipinput = models.CharField(max_length=100,  db_column='fv_ipinput')
    
    fn_pinid = models.IntegerField( db_column='fn_pinid')
    fv_alamatdomisili = models.TextField( db_column='fv_alamatdomisili')
    fv_kecamatan = models.CharField(max_length=155,  db_column='fv_kecamatan')
    fv_kelurahan = models.CharField(max_length=155,  db_column='fv_kelurahan')
    fv_rt = models.CharField(max_length=15,  db_column='fv_rt')
    fv_rw = models.CharField(max_length=15,  db_column='fv_rw')
    fv_emailpekerjaan = models.CharField(max_length=255,  db_column='fv_emailpekerjaan')
    fv_goldarah = models.CharField(max_length=3,  db_column='fv_goldarah')
    fv_ipk = models.CharField(max_length=15,  db_column='fv_ipk')
    fv_namaaAtNpwp = models.CharField(max_length=255,  db_column='fv_namaaAtNpwp')
    fv_alamatNpwp = models.TextField( db_column='fv_alamatNpwp')
    fv_nomorrekbca = models.CharField(max_length=25,  db_column='fv_nomorrekbca')
    fv_jeniskelamin = models.CharField(max_length=2,  db_column='fv_jeniskelamin')
    fd_tgllahir = models.DateField( db_column='fd_tgllahir')
    fv_foto = models.CharField(max_length=185,  db_column='fv_foto')
    fc_staff = models.CharField(max_length=1, default='F', db_column='fc_staff')
    
    fd_jammasuk = models.TimeField( db_column='fd_jammasuk')
    fd_jamistrht = models.IntegerField( db_column='fd_jamistrht')
    fd_jampulang = models.TimeField( db_column='fd_jampulang')
    fn_idjamkerja = models.IntegerField( db_column='fn_idjamkerja')
    
    fv_nomorKK = models.CharField(max_length=55,  db_column='fv_nomorKK')
    fn_kotalahir = models.IntegerField( db_column='fn_kotalahir')
    fv_pendidikanakhir = models.CharField(max_length=155,  db_column='fv_pendidikanakhir')
    fv_kampus = models.CharField(max_length=155,  db_column='fv_kampus')
    fv_alasanresign = models.TextField(null=True, blank=True, db_column='fv_alasanresign')
    fc_kategoriresign = models.CharField(max_length=55,null=True, blank=True, db_column='fc_kategoriresign')

    fn_userupdate = models.IntegerField(db_column='fn_userupdate')
    fd_dateupdate = models.DateTimeField(default=datetime.now, db_column='fd_dateupdate')
    fc_action = models.CharField(max_length=1,  db_column='fc_action')
    
    class Meta:
            db_table = 'hs_employees_tb'
            managed = False

class EmployeesspgTb(models.Model):
    fn_no_id = models.AutoField(primary_key=True, db_column='fn_no_id')
    fn_userid = models.IntegerField(db_column='fn_userid')
    fn_cusid = models.IntegerField(null=True, blank=True, db_column='fn_cusid')
    fn_idagama = models.IntegerField(null=True, blank=True, db_column='fn_idagama')
    fn_mirrageid = models.IntegerField(null=True, blank=True, db_column='fn_mirrageid')
    fn_anak = models.IntegerField(null=True, blank=True, db_column='fn_anak')
    fn_idbank = models.IntegerField(null=True, blank=True, db_column='fn_idbank')
    fn_ptkpid = models.IntegerField(null=True, blank=True, db_column='fn_ptkpid')
    
    fv_status_pekerja = models.CharField(max_length=255, null=True, blank=True, db_column='fv_status_pekerja')
    fv_type_pekerja = models.CharField(max_length=255, null=True, blank=True, db_column='fv_type_pekerja')
    fd_tglawalkontrak = models.DateTimeField(null=True, blank=True, db_column='fd_tglawalkontrak')
    fd_tglakhirkontrak = models.DateTimeField(null=True, blank=True, db_column='fd_tglakhirkontrak')
    
    fv_refkerja = models.CharField(max_length=55, null=True, blank=True, db_column='fv_refkerja')
    fv_divname = models.CharField(max_length=255, null=True, blank=True, db_column='fv_divname')
    fv_penempatan = models.CharField(max_length=255, null=True, blank=True, db_column='fv_penempatan')
    fv_nama_rekening = models.CharField(max_length=255, null=True, blank=True, db_column='fv_nama_rekening')
    fv_cabang_rekening = models.CharField(max_length=255, null=True, blank=True, db_column='fv_cabang_rekening')
    fv_keterangan = models.TextField(null=True, blank=True, db_column='fv_keterangan')
    
    fd_created_at = models.DateTimeField(auto_now_add=True, db_column='fd_created_at')
    fd_updated_at = models.DateTimeField(auto_now=True, db_column='fd_updated_at')
    
    fn_userid_in = models.IntegerField(null=True, blank=True, db_column='fn_userid_in')
    fn_userid_up = models.IntegerField(null=True, blank=True, db_column='fn_userid_up')

    class Meta:
        db_table = 'employeesspg_tb'
        managed = False


class TAksesusercust(models.Model):
    fn_idusercust = models.AutoField(primary_key=True, db_column='fn_idusercust')  # Jika ada Primary Key ID bawaan
    fn_userid = models.IntegerField(db_column='fn_userid')
    fn_cusid = models.IntegerField(db_column='fn_cusid')
    fc_status = models.CharField(max_length=1, default='T', db_column='fc_status')  # 'T' atau 'F'
    fn_userinput = models.IntegerField(null=True, blank=True, db_column='fn_userinput')
    fd_dateinput = models.DateTimeField(default=datetime.now(), db_column='fd_dateinput')

    class Meta:
        db_table = 't_aksesusercust'
        managed = False

class MCusStay(models.Model):
    fn_id = models.AutoField(primary_key=True, db_column='fn_id')
    fn_userid = models.IntegerField(db_column='fn_userid')
    fn_cusid = models.IntegerField(db_column='fn_cusid')
    fc_up_one = models.CharField(max_length=1, default='F', db_column='fc_up_one')  # 'T' atau 'F'
    fc_hold = models.CharField(max_length=1, default='F', db_column='fc_hold')
    fn_userid_in = models.IntegerField(null=True, blank=True, db_column='fn_userid_in')

    class Meta:
        db_table = 'm_cus_stay'
        managed = False

class ApprovalAbs(models.Model):
    fn_id = models.AutoField(primary_key=True)
    fn_userid = models.IntegerField()
    fd_tanggal = models.DateField()
    fv_shfit = models.CharField(max_length=55, null=True, blank=True)
    fd_jam_in = models.TimeField()
    fd_jam_out = models.TimeField()
    fv_tempat_in = models.CharField(max_length=255)
    fv_tempat_out = models.CharField(max_length=255)
    fv_note = models.TextField(null=True, blank=True)
    fc_approve = models.CharField(max_length=1)
    fn_userid_in = models.IntegerField()
    fn_userid_up = models.IntegerField()
    fd_create = models.DateTimeField(null=True, blank=True)
    fd_update = models.DateTimeField(null=True, blank=True)

    class Meta:
        managed = False  # Django tidak akan membuat atau mengubah tabel di DB
        db_table = 'approval_abs'