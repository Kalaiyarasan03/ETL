from django import forms

class UploadFileForm(forms.Form):
    file = forms.FileField()
    table_name = forms.CharField(max_length=100)
