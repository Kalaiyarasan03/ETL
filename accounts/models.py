from django.contrib.auth.models import AbstractUser
from django.db import models

class User(AbstractUser):
    class Roles(models.TextChoices):
        ENGINEER = 'ENGINEER', 'Engineer'
        MANAGER = 'MANAGER', 'Manager'
        ADMIN = 'ADMIN', 'Admin'

    role = models.CharField(max_length=20, choices=Roles.choices, default=Roles.ENGINEER)

    def is_engineer(self):
        return self.role == self.Roles.ENGINEER

    def is_manager(self):
        return self.role == self.Roles.MANAGER

    def is_admin(self):
        return self.role == self.Roles.ADMIN or self.is_superuser
