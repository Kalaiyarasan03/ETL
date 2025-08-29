from django.core.management.base import BaseCommand
from django.contrib.auth.models import User, Group
from etl_system.groups import create_groups

class Command(BaseCommand):
    help = 'Initialize ETL system with admin and engineer users'

    def handle(self, *args, **kwargs):
        # Create groups
        create_groups()
        self.stdout.write(self.style.SUCCESS('Created groups: ETL_Admin and ETL_Engineer'))
        
        # Create admin user
        if not User.objects.filter(username='admin').exists():
            admin_user = User.objects.create_superuser('admin', 'admin@example.com', 'admin123')
            admin_group = Group.objects.get(name='ETL_Admin')
            admin_user.groups.add(admin_group)
            self.stdout.write(self.style.SUCCESS('Created admin user: admin/admin123'))
        
        # Create engineer user
        if not User.objects.filter(username='engineer').exists():
            engineer_user = User.objects.create_user('engineer', 'engineer@example.com', 'engineer123')
            engineer_group = Group.objects.get(name='ETL_Engineer')
            engineer_user.groups.add(engineer_group)
            self.stdout.write(self.style.SUCCESS('Created engineer user: engineer/engineer123'))
        
        self.stdout.write(self.style.SUCCESS('ETL system initialization complete!'))