from django.contrib import admin
from django.urls import path, include

urlpatterns = [
    path('admin/', admin.site.urls),
    path('', include('etl_system.urls')),
    path("accounts/", include("accounts.urls")),
    path('excel/', include('excel_to_sql.urls', namespace='excel_to_sql')),

]