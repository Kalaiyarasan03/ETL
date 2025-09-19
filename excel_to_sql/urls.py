# excel_to_sql/urls.py

from django.urls import path
from . import views

app_name = 'excel_to_sql'  # <--- THIS is critical

urlpatterns = [
    path('upload/', views.upload_excel, name='upload_excel'),
    # other paths...
]
