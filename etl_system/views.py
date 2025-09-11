# etl_system/views.py
from django.shortcuts import render, redirect, get_object_or_404
from django.contrib.auth.decorators import login_required
from django.contrib.auth.mixins import LoginRequiredMixin
from django.urls import reverse_lazy
from django.views.generic import ListView, DetailView, CreateView, UpdateView, DeleteView
from django.contrib import messages
from django.http import JsonResponse, HttpResponse
from django.core.paginator import Paginator
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine, text
import subprocess
import sys
import os
from datetime import datetime, timedelta, date
from urllib.parse import quote_plus
from .models import source_info, srctbl_info, srcfile_info, table_schema, database_cred, execution_track
from .forms import SourceInfoForm, TableInfoForm, SourceFileInfoForm, TableSchemaForm, DatabaseCredForm, ExecutionTrackForm

def get_actual_db_engine_type(db_type):
    """
    Map custom database type names to actual database engine types
    This allows you to use custom names like 'mysql_wh', 'mysql_prod', etc.
    """
    # Convert to lowercase for comparison
    db_type_lower = db_type.lower().strip()
    
    # Define mapping of custom names to actual engine types
    CUSTOM_DB_TYPE_MAPPING = {
        # MySQL variants
        'mysql_wh': 'mysql',
        'mysql_warehouse': 'mysql',
        'mysql_prod': 'mysql',
        'mysql_staging': 'mysql',
        'mysql_dev': 'mysql',
        'vms': 'mysql',
        'chit_db': 'mysql',
        'warehouse_mysql': 'mysql',
        'prod_mysql': 'mysql',
        
        # PostgreSQL variants
        'postgres_wh': 'postgresql',
        'postgres_warehouse': 'postgresql',
        'postgres_prod': 'postgresql',
        'postgres_staging': 'postgresql',
        'pg_warehouse': 'postgresql',
        
        # Add more custom mappings as needed
        # 'your_custom_name': 'actual_engine_type',
    }
    
    # Check if it's a custom type that needs mapping
    if db_type_lower in CUSTOM_DB_TYPE_MAPPING:
        actual_type = CUSTOM_DB_TYPE_MAPPING[db_type_lower]
        print(f"Mapped custom database type '{db_type}' to engine type '{actual_type}'")
        return actual_type
    
    # If not in custom mapping, return original (for standard types)
    return db_type_lower

def get_db_engine(db_info):
    """Helper function to create database engine - FIXED VERSION with custom database type support"""
    # Get the actual engine type (handles custom names)
    actual_db_type = get_actual_db_engine_type(db_info['db_type'])
    
    # Define valid database types and their aliases
    MYSQL_TYPES = ['mysql', 'mariadb']
    POSTGRESQL_TYPES = ['postgresql', 'postgres', 'postgre']
    SQLITE_TYPES = ['sqlite', 'sqlite3']
    
    try:
        if actual_db_type in POSTGRESQL_TYPES:
            try:
                import psycopg2
                conn_str = f"postgresql+psycopg2://{db_info['username']}:{db_info['password']}@{db_info['host']}/{db_info['database']}"
            except ImportError:
                raise ImportError("psycopg2 is required for PostgreSQL connections")
                
        elif actual_db_type in MYSQL_TYPES:
            try:
                import pymysql
                port = db_info.get('port', 3306)
                # URL encode the password to handle special characters
                encoded_password = quote_plus(str(db_info['password']))
                conn_str = f"mysql+pymysql://{db_info['username']}:{encoded_password}@{db_info['host']}:{port}/{db_info['database']}"
            except ImportError:
                raise ImportError("pymysql is required for MySQL connections")
                
        elif actual_db_type in SQLITE_TYPES:
            conn_str = f"sqlite:///{db_info['database']}"
            
        else:
            # Create helpful error message with valid options
            valid_types = MYSQL_TYPES + POSTGRESQL_TYPES + SQLITE_TYPES
            raise ValueError(
                f"Unsupported database type: '{actual_db_type}' (mapped from '{db_info['db_type']}'). "
                f"Valid engine types are: {', '.join(valid_types)}. "
                f"Add your custom type '{db_info['db_type']}' to CUSTOM_DB_TYPE_MAPPING in get_actual_db_engine_type()."
            )
            
        return create_engine(conn_str)
        
    except Exception as e:
        print(f"Failed to create engine for {actual_db_type} (original: {db_info['db_type']}): {str(e)}")
        raise

# Dashboard View
@login_required
def dashboard(request):
    user = request.user
    is_admin = user.groups.filter(name='ETL_Admin').exists()
    
    context = {
        'is_admin': is_admin,
        'source_info_count': source_info.objects.count(),
        'table_info_count': srctbl_info.objects.count(),
        'source_file_count': srcfile_info.objects.count(),
        'table_schema_count': table_schema.objects.count(),
        'database_cred_count': database_cred.objects.count(),
    }
    
    if is_admin:
        context['database_cred_count'] = database_cred.objects.count(),
    
    return render(request, 'etl_system/dashboard.html', context)

# ETL Execution and Data Loading Views
@login_required
def etl_execution(request):
    """View to manage ETL execution"""
    sources = source_info.objects.all()
    return render(request, 'etl_system/ETL_Execution/etl_execution.html', {'sources': sources})

@login_required
def execute_etl(request, datasrc_id):
    """Execute ETL for a specific data source"""
    if request.method == 'POST':
        try:
            # Get the path to load_table.py
            script_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'load_table.py')
            
            # Execute the ETL script
            result = subprocess.run([
                sys.executable, script_path, str(datasrc_id)
            ], capture_output=True, text=True, timeout=300)
            
            if result.returncode == 0:
                messages.success(request, f'ETL execution completed successfully for Data Source ID: {datasrc_id}')
                return JsonResponse({
                    'status': 'success',
                    'message': f'ETL completed for Data Source ID: {datasrc_id}',
                    'output': result.stdout
                })
            else:
                messages.error(request, f'ETL execution failed: {result.stderr}')
                return JsonResponse({
                    'status': 'error',
                    'message': f'ETL failed: {result.stderr}',
                    'output': result.stdout
                })
                
        except subprocess.TimeoutExpired:
            messages.error(request, 'ETL execution timed out')
            return JsonResponse({
                'status': 'error',
                'message': 'ETL execution timed out'
            })
        except Exception as e:
            messages.error(request, f'Error executing ETL: {str(e)}')
            return JsonResponse({
                'status': 'error',
                'message': f'Error executing ETL: {str(e)}'
            })
    
    return redirect('etl_execution')

@login_required
def data_viewer(request):
    """View to display available tables for data viewing"""
    try:
        tables = srctbl_info.objects.all()
        return render(request, 'etl_system/Data_Viewer/data_viewer.html', {'tables': tables})
    except Exception as e:
        messages.error(request, f'Error loading tables: {str(e)}')
        return render(request, 'etl_system/Data_Viewer/data_viewer.html', {'tables': []})

@login_required
def view_table_data(request, table_id):
    """View data from a specific table - Updated to support custom database types"""
    try:
        # Get table information
        table_info = get_object_or_404(srctbl_info, SRCTBL_ID=table_id)
        print(f"Processing table: {table_info.TGT_TABLENAME}")
        
        # Get target database credentials with role-based lookup
        db_cred = database_cred.objects.filter(
            db_type__iexact=table_info.TGT_DATABASE,
            db_role__iexact='target'
        ).first()
        
        # Fallback to any credential with matching db_type if role-specific not found
        if not db_cred:
            db_cred = database_cred.objects.filter(db_type__iexact=table_info.TGT_DATABASE).first()
            
        if not db_cred:
            messages.error(request, f'No credentials found for database type: {table_info.TGT_DATABASE}')
            return redirect('data_viewer')
        
        print(f"Found credentials for: {db_cred.db_type} (role: {db_cred.db_role}, database: {db_cred.database})")
        
        # Create database engine - Now supports custom database types
        try:
            engine = get_db_engine({
                'db_type': db_cred.db_type,
                'host': db_cred.host,
                'port': db_cred.port,
                'database': db_cred.database,
                'username': db_cred.username,
                'password': db_cred.password
            })
            print("Database engine created successfully")
        except Exception as e:
            messages.error(request, f'Database connection error: {str(e)}')
            return redirect('data_viewer')
        
        # Build the full table name
        if table_info.TGT_SCHEMA and table_info.TGT_SCHEMA.strip():
            full_table_name = f"`{table_info.TGT_SCHEMA}`.`{table_info.TGT_TABLENAME}`"
        else:
            full_table_name = f"`{table_info.TGT_TABLENAME}`"
        
        print(f"Full table name: {full_table_name}")
        
        # Get pagination parameters
        page = int(request.GET.get('page', 1))
        per_page = int(request.GET.get('per_page', 50))
        search = request.GET.get('search', '').strip()
        
        # Build base queries
        base_query = f"SELECT * FROM {full_table_name}"
        count_query = f"SELECT COUNT(*) as total FROM {full_table_name}"
        
        # Add search filter if provided
        where_clause = ""
        if search:
            try:
                # Get table schema to build search conditions for text columns
                schemas = table_schema.objects.filter(
                    SRCTBL_ID=table_id, 
                    SRC_TRG_IND='TRG'
                ).exclude(DATA_TYPE__isnull=True)
                
                search_conditions = []
                for schema in schemas:
                    data_type = schema.DATA_TYPE.lower() if schema.DATA_TYPE else ""
                    if any(text_type in data_type for text_type in ['varchar', 'char', 'text', 'string']):
                        column_name = schema.COLUMN_NM
                        search_conditions.append(f"`{column_name}` LIKE '%{search}%'")
                
                if search_conditions:
                    where_clause = " WHERE " + " OR ".join(search_conditions)
                    base_query += where_clause
                    count_query += where_clause
                    print(f"Search applied: {where_clause}")
                    
            except Exception as e:
                print(f"Search filter error: {str(e)}")
                # Continue without search if there's an error
                
        # Get total count
        try:
            with engine.connect() as conn:
                result = conn.execute(text(count_query))
                total_count = result.fetchone()[0]
            print(f"Total count: {total_count}")
        except Exception as e:
            print(f"Count query error: {str(e)}")
            messages.error(request, f'Error counting records: {str(e)}')
            return redirect('data_viewer')
        
        # Calculate pagination
        offset = (page - 1) * per_page
        data_query = f"{base_query} LIMIT {per_page} OFFSET {offset}"
        
        print(f"Data query: {data_query}")
        
        # Fetch data
        try:
            df = pd.read_sql(data_query, engine)
            print(f"Fetched {len(df)} rows")
        except Exception as e:
            print(f"Data fetch error: {str(e)}")
            messages.error(request, f'Error fetching data: {str(e)}')
            return redirect('data_viewer')
        finally:
            engine.dispose()
        
        # Create pagination info
        total_pages = (total_count + per_page - 1) // per_page
        has_previous = page > 1
        has_next = page < total_pages
        
        pagination_info = {
            'page': page,
            'per_page': per_page,
            'total_count': total_count,
            'total_pages': total_pages,
            'has_previous': has_previous,
            'has_next': has_next,
            'previous_page': page - 1 if has_previous else None,
            'next_page': page + 1 if has_next else None,
        }
        
        # Convert DataFrame to list of dictionaries for template
        data = df.to_dict('records') if not df.empty else []
        columns = df.columns.tolist() if not df.empty else []
        
        context = {
            'table_info': table_info,
            'data': data,
            'columns': columns,
            'pagination': pagination_info,
            'search': search,
            'per_page': per_page,
        }
        
        return render(request, 'etl_system/Data_Viewer/table_data.html', context)
        
    except Exception as e:
        print(f"Unexpected error in view_table_data: {str(e)}")
        import traceback
        traceback.print_exc()
        messages.error(request, f'Unexpected error: {str(e)}')
        return redirect('data_viewer')

@login_required
def export_table_data(request, table_id):
    """Export table data to CSV - Updated to support custom database types"""
    try:
        table_info = get_object_or_404(srctbl_info, SRCTBL_ID=table_id)
        
        # Get target database credentials with role-based lookup
        db_cred = database_cred.objects.filter(
            db_type__iexact=table_info.TGT_DATABASE,
            db_role__iexact='target'
        ).first()
        
        # Fallback to any credential with matching db_type if role-specific not found
        if not db_cred:
            db_cred = database_cred.objects.filter(db_type__iexact=table_info.TGT_DATABASE).first()
            
        if not db_cred:
            messages.error(request, f'No credentials found for database type: {table_info.TGT_DATABASE}')
            return redirect('data_viewer')
        
        # Create database engine - Now supports custom database types
        engine = get_db_engine({
            'db_type': db_cred.db_type,
            'host': db_cred.host,
            'port': db_cred.port,
            'database': db_cred.database,
            'username': db_cred.username,
            'password': db_cred.password
        })
        
        # Build the full table name
        if table_info.TGT_SCHEMA and table_info.TGT_SCHEMA.strip():
            full_table_name = f"`{table_info.TGT_SCHEMA}`.`{table_info.TGT_TABLENAME}`"
        else:
            full_table_name = f"`{table_info.TGT_TABLENAME}`"
        
        # Fetch all data
        query = f"SELECT * FROM {full_table_name}"
        df = pd.read_sql(query, engine)
        engine.dispose()
        
        # Create HTTP response with CSV
        response = HttpResponse(content_type='text/csv')
        response['Content-Disposition'] = f'attachment; filename="{table_info.TGT_TABLENAME}_{date.today()}.csv"'
        
        # Write CSV to response
        df.to_csv(path_or_buf=response, index=False)
        
        return response
        
    except Exception as e:
        print(f"Export error: {str(e)}")
        messages.error(request, f'Error exporting data: {str(e)}')
        return redirect('data_viewer')

# Execution Track Views (for monitoring ETL executions)
class ExecutionTrackListView(LoginRequiredMixin, ListView):
    model = execution_track
    template_name = 'etl_system/Execution_Track/execution_track_list.html'
    context_object_name = 'executions'
    ordering = ['-EXECUTION_DT', 'SRCTBL_ID']
    paginate_by = 50

    def get_queryset(self):
        queryset = super().get_queryset()
        # Filter by completion status if requested
        status = self.request.GET.get('status')
        if status:
            queryset = queryset.filter(COMPLETE_TRACK=status)
        
        # Filter by date range if requested
        start_date = self.request.GET.get('start_date')
        end_date = self.request.GET.get('end_date')
        if start_date:
            queryset = queryset.filter(EXECUTION_DT__gte=start_date)
        if end_date:
            queryset = queryset.filter(EXECUTION_DT__lte=end_date)
            
        return queryset

class ExecutionTrackDetailView(LoginRequiredMixin, DetailView):
    model = execution_track
    template_name = 'etl_system/Execution_Track/execution_track_detail.html'
    context_object_name = 'execution'

@login_required
def execution_summary(request):
    """View to show execution summary and statistics"""
    from django.db.models import Count, Sum, Q
    
    # Get recent executions (last 30 days)
    thirty_days_ago = datetime.now().date() - timedelta(days=30)
    
    recent_executions = execution_track.objects.filter(
        EXECUTION_DT__gte=thirty_days_ago
    )
    
    # Calculate statistics
    total_executions = recent_executions.count()
    successful_executions = recent_executions.filter(COMPLETE_TRACK='Y').count()
    failed_executions = recent_executions.filter(COMPLETE_TRACK='N').count()
    total_records_loaded = recent_executions.aggregate(
        total=Sum('REC_LOAD_COUNT')
    )['total'] or 0
    
    # Get execution summary by table
    table_summary = recent_executions.values('SRCTBL_ID').annotate(
        execution_count=Count('SRCTBL_ID'),
        success_count=Count('SRCTBL_ID', filter=Q(COMPLETE_TRACK='Y')),
        total_records=Sum('REC_LOAD_COUNT')
    ).order_by('-execution_count')[:10]
    
    # Get daily execution counts for the last 7 days
    seven_days_ago = datetime.now().date() - timedelta(days=7)
    daily_executions = recent_executions.filter(
        EXECUTION_DT__gte=seven_days_ago
    ).values('EXECUTION_DT').annotate(
        count=Count('EXECUTION_DT'),
        success_count=Count('EXECUTION_DT', filter=Q(COMPLETE_TRACK='Y'))
    ).order_by('EXECUTION_DT')
    
    context = {
        'total_executions': total_executions,
        'successful_executions': successful_executions,
        'failed_executions': failed_executions,
        'success_rate': round((successful_executions / total_executions * 100) if total_executions > 0 else 0, 1),
        'total_records_loaded': total_records_loaded,
        'table_summary': table_summary,
        'daily_executions': daily_executions,
        'thirty_days_ago': thirty_days_ago,
    }
    
    return render(request, 'etl_system/Execution_Track/execution_summary.html', context)

# Source Info Views
class SourceInfoListView(LoginRequiredMixin, ListView):
    model = source_info
    template_name = 'etl_system/Sources_Info/source_info_list.html'
    context_object_name = 'sources'

class SourceInfoDetailView(LoginRequiredMixin, DetailView):
    model = source_info
    template_name = 'etl_system/Sources_Info/source_info_detail.html'
    context_object_name = 'source'

class SourceInfoCreateView(LoginRequiredMixin, CreateView):
    model = source_info
    form_class = SourceInfoForm
    template_name = 'etl_system/Sources_Info/source_info_form.html'
    success_url = reverse_lazy('source_info_list')

class SourceInfoUpdateView(LoginRequiredMixin, UpdateView):
    model = source_info
    form_class = SourceInfoForm
    template_name = 'etl_system/Sources_Info/source_info_form.html'
    success_url = reverse_lazy('source_info_list')

class SourceInfoDeleteView(LoginRequiredMixin, DeleteView):
    model = source_info
    template_name = 'etl_system/Sources_Info/source_info_confirm_delete.html'
    success_url = reverse_lazy('source_info_list')
    

# Table Info Views
class TableInfoListView(LoginRequiredMixin, ListView):
    model = srctbl_info
    template_name = 'etl_system/Tables_Info/table_info_list.html'
    context_object_name = 'tables'

class TableInfoDetailView(LoginRequiredMixin, DetailView):
    model = srctbl_info
    template_name = 'etl_system/Tables_Info/table_info_detail.html'
    context_object_name = 'table'

class TableInfoCreateView(LoginRequiredMixin, CreateView):
    model = srctbl_info
    form_class = TableInfoForm
    template_name = 'etl_system/Tables_Info/table_info_form.html'
    success_url = reverse_lazy('table_info_list')

class TableInfoUpdateView(LoginRequiredMixin, UpdateView):
    model = srctbl_info
    form_class = TableInfoForm
    template_name = 'etl_system/Tables_Info/table_info_form.html'
    success_url = reverse_lazy('table_info_list')

class TableInfoDeleteView(LoginRequiredMixin, DeleteView):
    model = srctbl_info
    template_name = 'etl_system/Tables_Info/table_info_confirm_delete.html'
    success_url = reverse_lazy('table_info_list')

# Source File Info Views
class SourceFileInfoListView(LoginRequiredMixin, ListView):
    model = srcfile_info
    template_name = 'etl_system/Source_File_Info/source_file_info_list.html'
    context_object_name = 'files'

class SourceFileInfoDetailView(LoginRequiredMixin, DetailView):
    model = srcfile_info
    template_name = 'etl_system/Source_File_Info/source_file_info_detail.html'
    context_object_name = 'file'

class SourceFileInfoCreateView(LoginRequiredMixin, CreateView):
    model = srcfile_info
    form_class = SourceFileInfoForm
    template_name = 'etl_system/Source_File_Info/source_file_info_form.html'
    success_url = reverse_lazy('source_file_info_list')

class SourceFileInfoUpdateView(LoginRequiredMixin, UpdateView):
    model = srcfile_info
    form_class = SourceFileInfoForm
    template_name = 'etl_system/Source_File_Info/source_file_info_form.html'
    success_url = reverse_lazy('source_file_info_list')

class SourceFileInfoDeleteView(LoginRequiredMixin, DeleteView):
    model = srcfile_info
    template_name = 'etl_system/Source_File_Info/source_file_info_confirm_delete.html'
    success_url = reverse_lazy('source_file_info_list')

# Table Schema Views
class TableSchemaListView(LoginRequiredMixin, ListView):
    model = table_schema
    template_name = 'etl_system/Table_Schema/table_schema_list.html'
    context_object_name = 'schemas'

class TableSchemaDetailView(LoginRequiredMixin, DetailView):
    model = table_schema
    template_name = 'etl_system/Table_Schema/table_schema_detail.html'
    context_object_name = 'schema'

class TableSchemaCreateView(LoginRequiredMixin, CreateView):
    model = table_schema
    form_class = TableSchemaForm
    template_name = 'etl_system/Table_Schema/table_schema_form.html'
    success_url = reverse_lazy('table_schema_list')

class TableSchemaUpdateView(LoginRequiredMixin, UpdateView):
    model = table_schema
    form_class = TableSchemaForm
    template_name = 'etl_system/Table_Schema/table_schema_form.html'
    success_url = reverse_lazy('table_schema_list')

class TableSchemaDeleteView(LoginRequiredMixin, DeleteView):
    model = table_schema
    template_name = 'etl_system/Table_Schema/table_schema_confirm_delete.html'
    success_url = reverse_lazy('table_schema_list')

# Database Credential Views (Admin Only)
class DatabaseCredListView(LoginRequiredMixin, ListView):
    model = database_cred
    template_name = 'etl_system/Database_Info/database_cred_list.html'
    context_object_name = 'credentials'

class DatabaseCredDetailView(LoginRequiredMixin, DetailView):
    model = database_cred
    template_name = 'etl_system/Database_Info/database_cred_detail.html'
    context_object_name = 'credential'

class DatabaseCredCreateView(LoginRequiredMixin, CreateView):
    model = database_cred
    form_class = DatabaseCredForm
    template_name = 'etl_system/Database_Info/database_cred_form.html'
    success_url = reverse_lazy('database_cred_list')

class DatabaseCredUpdateView(LoginRequiredMixin, UpdateView):
    model = database_cred
    form_class = DatabaseCredForm
    template_name = 'etl_system/Database_Info/database_cred_form.html'
    success_url = reverse_lazy('database_cred_list')

class DatabaseCredDeleteView(LoginRequiredMixin, DeleteView):
    model = database_cred
    template_name = 'etl_system/Database_Info/database_cred_confirm_delete.html'
    success_url = reverse_lazy('database_cred_list')