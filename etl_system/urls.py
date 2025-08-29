# etl_system/urls.py
from django.urls import path
from django.contrib.auth import views as auth_views
from . import views
from .forms import CustomAuthenticationForm

urlpatterns = [
    # Authentication
    path('login/', auth_views.LoginView.as_view(
        template_name='etl_system/login.html',
        authentication_form=CustomAuthenticationForm
    ), name='login'),
    path('logout/', auth_views.LogoutView.as_view(
        next_page='login', 
        http_method_names=['get', 'post']  
    ), name='logout'),
    
    # Dashboard
    path('', views.dashboard, name='dashboard'),
    
    # ETL Execution URLs
    path('etl-execution/', views.etl_execution, name='etl_execution'),
    path('etl-execution/execute/<int:datasrc_id>/', views.execute_etl, name='execute_etl'),
    
    # Data Viewer URLs
    path('data-viewer/', views.data_viewer, name='data_viewer'),
    path('data-viewer/table/<int:table_id>/', views.view_table_data, name='view_table_data'),
    path('data-viewer/export/<int:table_id>/', views.export_table_data, name='export_table_data'),
    
    # Source Info URLs
    path('sources/', views.SourceInfoListView.as_view(), name='source_info_list'),
    path('sources/<int:pk>/', views.SourceInfoDetailView.as_view(), name='source_info_detail'),
    path('sources/new/', views.SourceInfoCreateView.as_view(), name='source_info_create'),
    path('sources/<int:pk>/edit/', views.SourceInfoUpdateView.as_view(), name='source_info_update'),
    path('sources/<int:pk>/delete/', views.SourceInfoDeleteView.as_view(), name='source_info_delete'),
    
    # Table Info URLs
    path('tables/', views.TableInfoListView.as_view(), name='table_info_list'),
    path('tables/<int:pk>/', views.TableInfoDetailView.as_view(), name='table_info_detail'),
    path('tables/new/', views.TableInfoCreateView.as_view(), name='table_info_create'),
    path('tables/<int:pk>/edit/', views.TableInfoUpdateView.as_view(), name='table_info_update'),
    path('tables/<int:pk>/delete/', views.TableInfoDeleteView.as_view(), name='table_info_delete'),
    
    # Source File Info URLs
    path('files/', views.SourceFileInfoListView.as_view(), name='source_file_info_list'),
    path('files/<int:pk>/', views.SourceFileInfoDetailView.as_view(), name='source_file_info_detail'),
    path('files/new/', views.SourceFileInfoCreateView.as_view(), name='source_file_info_create'),
    path('files/<int:pk>/edit/', views.SourceFileInfoUpdateView.as_view(), name='source_file_info_update'),
    path('files/<int:pk>/delete/', views.SourceFileInfoDeleteView.as_view(), name='source_file_info_delete'),
    
    # Table Schema URLs
    path('schemas/', views.TableSchemaListView.as_view(), name='table_schema_list'),
    path('schemas/<int:pk>/', views.TableSchemaDetailView.as_view(), name='table_schema_detail'),
    path('schemas/new/', views.TableSchemaCreateView.as_view(), name='table_schema_create'),
    path('schemas/<int:pk>/edit/', views.TableSchemaUpdateView.as_view(), name='table_schema_update'),
    path('schemas/<int:pk>/delete/', views.TableSchemaDeleteView.as_view(), name='table_schema_delete'),
    
    # Database Credential URLs (Admin Only)
    path('credentials/', views.DatabaseCredListView.as_view(), name='database_cred_list'),
    path('credentials/<int:pk>/', views.DatabaseCredDetailView.as_view(), name='database_cred_detail'),
    path('credentials/new/', views.DatabaseCredCreateView.as_view(), name='database_cred_create'),
    path('credentials/<int:pk>/edit/', views.DatabaseCredUpdateView.as_view(), name='database_cred_update'),
    path('credentials/<int:pk>/delete/', views.DatabaseCredDeleteView.as_view(), name='database_cred_delete'),

    # Execution Track URLs (Optional - for monitoring)
    path('executions/', views.ExecutionTrackListView.as_view(), name='execution_track_list'),
    path('executions/<int:pk>/', views.ExecutionTrackDetailView.as_view(), name='execution_track_detail'),
    path('executions/summary/', views.execution_summary, name='execution_summary'),
]