# etl_system/context_processors.py
def user_roles(request):
    user = request.user
    return {
        'is_admin': user.is_authenticated and user.is_superuser,
        'is_manager': user.is_authenticated and user.groups.filter(name='Manager').exists(),
    }
