from django.contrib.auth.models import Group, Permission
from django.contrib.contenttypes.models import ContentType
from .models import SourceInfo, TableInfo, SourceFileInfo, TableSchema, DatabaseCred

def create_groups():
    # For Admins
    admin_group, created = Group.objects.get_or_create(name = 'ETL_Admin')
    #For Engineers
    engineer_group, created = Group.objects.get_or_create(name = "ETL_Engineer")

    #Get content types for models
    source_info_ct = ContentType.objects.get_for_model(SourceInfo)
    table_info_ct = ContentType.objects.get_for_model(TableInfo)
    source_file_info_ct = ContentType.objects.get_for_model(SourceFileInfo)
    table_schema_ct = ContentType.objects.get_for_model(TableSchema)
    database_cred_ct = ContentType.objects.get_for_model(DatabaseCred)

    # Define permissions for engineer (can access everything except DatabaseCred)
    engineer_permissions = Permission.objects.filter(
        content_type__in=[source_info_ct, table_info_ct, source_file_info_ct, table_schema_ct]
    )
    
    # Add permissions to engineer group
    for perm in engineer_permissions:
        engineer_group.permissions.add(perm)
    
    # Define admin permissions (all models including DatabaseCred)
    admin_permissions = Permission.objects.filter(
        content_type__in=[source_info_ct, table_info_ct, source_file_info_ct, table_schema_ct, database_cred_ct]
    )
    
    # Add permissions to admin group
    for perm in admin_permissions:
        admin_group.permissions.add(perm)