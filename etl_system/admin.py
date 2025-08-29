from django.contrib import admin
from .models import source_info, srctbl_info, srcfile_info, table_schema, database_cred, execution_track

@admin.register(source_info)
class SourceInfoAdmin(admin.ModelAdmin):
    list_display = ['SOURCE_ID', 'SOURCE_NM', 'SOURCE_TYP', 'EXTRCT_MTHD']

@admin.register(srctbl_info)
class TableInfoAdmin(admin.ModelAdmin):
    list_display = ['SRCTBL_ID', 'SRC_TABLENAME', 'TGT_TABLENAME', 'REF_FRQNCY']

@admin.register(execution_track)
class ExecutionTrackAdmin(admin.ModelAdmin):
    list_display = ['EXECUTION_DT', 'SRCTBL_ID', 'COMPLETE_TRACK', 'REC_LOAD_COUNT']
    list_filter = ['EXECUTION_DT', 'COMPLETE_TRACK']