from django.shortcuts import render
from .forms import UploadFileForm
from .utils import upload_excel_dynamic, get_mysql_url
import tempfile
import os
import logging
from django.contrib.auth.decorators import user_passes_test


logger = logging.getLogger(__name__)

ALLOWED_EXTENSIONS = {'.xlsx', '.xls', '.csv'}

@user_passes_test(lambda u: u.is_authenticated and u.role == 'ADMIN')
def upload_excel(request):
    context = {"result": None, "success": False, "error": None}

    if request.method == "POST":
        form = UploadFileForm(request.POST, request.FILES)
        if form.is_valid():
            f = request.FILES['file']
            table_name = form.cleaned_data['table_name']
            mysql_url = get_mysql_url()

            ext = os.path.splitext(f.name)[1].lower()
            if ext not in ALLOWED_EXTENSIONS:
                context["error"] = (
                    f"Unsupported file extension '{ext}'. "
                    f"Allowed extensions are: {', '.join(ALLOWED_EXTENSIONS)}"
                )
            else:
                # Save uploaded file temporarily
                with tempfile.NamedTemporaryFile(delete=False, suffix=ext) as tmp:
                    for chunk in f.chunks():
                        tmp.write(chunk)
                    tmp_path = tmp.name

                try:
                    result = upload_excel_dynamic(tmp_path, table_name, mysql_url)
                    context["result"] = result
                    context["success"] = True
                except Exception as e:
                    logger.exception("Failed to upload file")
                    context["error"] = f"Error processing file: {e}"
                finally:
                    # Clean up temporary file
                    try:
                        os.remove(tmp_path)
                    except OSError:
                        logger.warning(f"Could not delete temporary file {tmp_path}")
        else:
            context["error"] = "Invalid form submission."
    else:
        form = UploadFileForm()

    context["form"] = form
    return render(request, "etl_system/upload.html", context)
