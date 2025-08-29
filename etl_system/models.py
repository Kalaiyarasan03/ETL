# etl_system/models.py
from django.db import models
from django.contrib.auth.models import User

class database_cred(models.Model):
    db_type = models.CharField(max_length=50)
    db_role = models.CharField(max_length=50)
    host = models.CharField(max_length=255)
    port = models.IntegerField()
    database = models.CharField(max_length=255)
    username = models.CharField(max_length=255)
    password = models.CharField(max_length=255)
    
    def __str__(self):
        return f"{self.db_type} - {self.db_role}"
    
    class Meta:
        db_table = 'database_cred'
        verbose_name = 'Database Credential'
        verbose_name_plural = 'Database Credentials'

class source_info(models.Model):
    SOURCE_ID = models.AutoField(primary_key=True)
    SOURCE_NM = models.CharField(max_length=64, null=True, blank=True)
    SOURCE_TYP = models.CharField(max_length=64, null=True, blank=True)
    USERID = models.CharField(max_length=64, null=True, blank=True)
    USERPSWRD = models.CharField(max_length=64, null=True, blank=True)
    EXTRCT_MTHD = models.CharField(max_length=8, null=True, blank=True)
    
    def __str__(self):
        return f"{self.SOURCE_NM}"
    
    class Meta:
        db_table = 'source_info'
        verbose_name = 'Source Information'
        verbose_name_plural = 'Source Information'

class srctbl_info(models.Model):
    SRCTBL_ID = models.AutoField(primary_key=True)  # Changed from AutoField to IntegerField
    DATASRC_ID = models.IntegerField()
    SRC_DATABASE = models.CharField(max_length=256, null=True, blank=True)  # Increased length
    SRC_SCHEMA = models.CharField(max_length=256, null=True, blank=True)    # Increased length
    SRC_TABLENAME = models.CharField(max_length=256, null=True, blank=True) # Increased length
    TGT_DATABASE = models.CharField(max_length=256, null=True, blank=True)  # Increased length
    TGT_SCHEMA = models.CharField(max_length=256, null=True, blank=True)    # Increased length
    TGT_TABLENAME = models.CharField(max_length=256, null=True, blank=True) # Increased length
    REF_FRQNCY = models.CharField(max_length=8, null=True, blank=True)      # Added new field
    SELECT_QUERY=models.CharField(max_length=8, null=True, blank=True)
    FILTER_COND= models.CharField(max_length=3, null=True, blank=True)
    RECON_REQ=models.CharField(max_length=3, null=True, blank=True)
    TRG_TABLE_SFX=models.CharField(max_length=8, null=True, blank=True)
    LOAD_TYPE=models.CharField(max_length=8, null=True, blank=True)
    ACTIVE_IND=models.CharField(max_length=1, null=True, blank=True)
    
    def __str__(self):
        return f"{self.SRC_TABLENAME} -> {self.TGT_TABLENAME}"
    
    class Meta:
        db_table = 'srctbl_info'
        verbose_name = 'Table Information'
        verbose_name_plural = 'Table Information'

class srcfile_info(models.Model):
    SRCFILE_ID = models.AutoField(primary_key=True)
    DATASRC_ID = models.IntegerField()
    SRC_DIRECTORY = models.CharField(max_length=255, null=True, blank=True)
    SRC_FILENAME = models.CharField(max_length=255, null=True, blank=True)
    TGT_DATABASE = models.CharField(max_length=64, null=True, blank=True)
    TGT_TABLENAME = models.CharField(max_length=64, null=True, blank=True)
    SRCLAYOUT_ID = models.IntegerField()
    TRGLAYOUT_ID = models.IntegerField()
    
    def __str__(self):
        return f"{self.SRC_FILENAME}"
    
    class Meta:
        db_table = 'srcfile_info'
        verbose_name = 'Source File Information'
        verbose_name_plural = 'Source File Information'

class table_schema(models.Model):
    SRCTBL_ID = models.IntegerField(primary_key=True)
    COLUMN_SEQ = models.IntegerField()
    SRC_TRG_IND = models.CharField(max_length=3)
    SCHEMA_NM = models.CharField(max_length=64, null=True, blank=True)
    TABLE_NM = models.CharField(max_length=64, null=True, blank=True)
    COLUMN_NM = models.CharField(max_length=64, null=True, blank=True)
    DATA_TYPE = models.CharField(max_length=64, null=True, blank=True)
    XFRMTN = models.CharField(max_length=128, null=True, blank=True)
    PRIMARY_KEY = models.CharField(max_length=1, null=True, blank=True)
    DEFAULT_VAL = models.CharField(max_length=64, null=True, blank=True)
    
    def __str__(self):
        return f"{self.TABLE_NM}.{self.COLUMN_NM}"
    
    class Meta:
        db_table = 'table_schema'
        verbose_name = 'Table Schema'
        verbose_name_plural = 'Table Schemas'
        unique_together = (('SRCTBL_ID', 'COLUMN_SEQ', 'SRC_TRG_IND'),)

class execution_track(models.Model):
    EXECUTION_DT = models.DateField()
    SRCTBL_ID = models.IntegerField()
    COMPLETE_TRACK = models.CharField(max_length=1, default='N')
    REC_LOAD_COUNT = models.BigIntegerField(null=True, blank=True)
    LAST_EXEC_DT = models.DateField(default='2000-01-01')
    NEXT_EXEC_DT = models.DateField(default='2099-12-31')
    
    def __str__(self):
        return f"Execution {self.EXECUTION_DT} - Table {self.SRCTBL_ID}"
    
    class Meta:
        db_table = 'execution_track'
        verbose_name = 'Execution Track'
        verbose_name_plural = 'Execution Tracks'
        # Note: Django doesn't support composite primary keys directly
        # You may need to handle uniqueness at the application level
        unique_together = (('EXECUTION_DT', 'SRCTBL_ID'),)