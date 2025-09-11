from django.contrib.auth import get_user_model
from django.contrib.auth.decorators import login_required, user_passes_test
from django.shortcuts import render, redirect, get_object_or_404
from .forms import UserForm

from django.contrib.auth import get_user_model

User = get_user_model()
  # ✅ This uses your custom user model

def admin_only(user):
    return user.is_authenticated and user.is_superuser  # Or use `user.is_admin()` if defined

@login_required
@user_passes_test(admin_only, login_url='/')
def user_list(request):
    users = User.objects.all()
    return render(request, "etl_system/user_list.html", {"users": users})

@login_required
@user_passes_test(admin_only, login_url='/')
def user_edit(request, pk):
    user = get_object_or_404(User, pk=pk)
    if request.method == "POST":
        form = UserForm(request.POST, instance=user)
        if form.is_valid():
            form.save()
            return redirect("user_list")
    else:
        form = UserForm(instance=user)
    return render(request, "etl_system/user_form.html", {"form": form})

from .forms import UserForm, CustomUserCreationForm  # import your custom creation form

@login_required
@user_passes_test(admin_only, login_url='/')
def user_create(request):
    if request.method == 'POST':
        form = CustomUserCreationForm(request.POST)  # use custom form here
        if form.is_valid():
            user = form.save()
            return redirect('user_list')
    else:
        form = CustomUserCreationForm()
    return render(request, 'etl_system/user_create.html', {'form': form})

def user_delete(request, pk):
    user = get_object_or_404(User, pk=pk)
    if request.method == "POST":
        user.delete()
        return redirect('user_list')
    return render(request, 'etl_system/user_confirm_delete.html', {'user': user})
