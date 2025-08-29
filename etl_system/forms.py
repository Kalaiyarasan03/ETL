# etl_system/forms.py
from django import forms
from django.contrib.auth.forms import AuthenticationForm
from .models import source_info, srctbl_info, srcfile_info, table_schema, database_cred, execution_track

class CustomAuthenticationForm(AuthenticationForm):
    username = forms.CharField(widget=forms.TextInput(attrs={'class': 'form-control', 'placeholder': 'Username'}))
    password = forms.CharField(widget=forms.PasswordInput(attrs={'class': 'form-control', 'placeholder': 'Password'}))

class SourceInfoForm(forms.ModelForm):
    class Meta:
        model = source_info
        fields = '__all__'
        widgets = {
            'SOURCE_NM': forms.TextInput(attrs={'class': 'form-control'}),
            'SOURCE_TYP': forms.TextInput(attrs={'class': 'form-control'}),
            'USERID': forms.TextInput(attrs={'class': 'form-control'}),
            'USERPSWRD': forms.PasswordInput(attrs={'class': 'form-control'}),
            'EXTRCT_MTHD': forms.TextInput(attrs={'class': 'form-control'}),
        }

class TableInfoForm(forms.ModelForm):

    YES_NO_CHOICES = [
         ('', 'Select Yes/No'),
        ('Y', 'Yes'),
        ('N', 'No'),
    ]
    FILTER_COND= forms.ChoiceField(
        choices=YES_NO_CHOICES,
        widget=forms.Select(attrs={'class': 'form-control'}),
        label="YES NO"
        )
    Y_N_CHOICES = [
         ('', 'Select Y/N'),
        ('Y', 'Y'),
        ('N', 'N'),
    ]
    ACTIVE_IND= forms.ChoiceField(
    choices=Y_N_CHOICES,
    widget=forms.Select(attrs={'class': 'form-control'}),
    label="Y/N"
    )
    
    REFRESH_CHOICES = [
        ('', 'Select Frequency'),  # optional placeholder
        ('DAILY', 'DAILY'),
        ('WEEKLY', 'WEEKLY'),
        ('MONTHLY', 'MONTHLY'),
    ]
    REF_FRQNCY = forms.ChoiceField(
        choices=REFRESH_CHOICES,
        widget=forms.Select(attrs={'class': 'form-control'}),
        label="Refresh Frequency"
        )
    
    DATASRC_ID = forms.ChoiceField(
        choices=[],
        widget=forms.Select(attrs={'class': 'form-control'}),
        label="Data Source ID"
    )
    SRC_DATABASE = forms.ChoiceField(
        choices=[],
        widget=forms.Select(attrs={'class': 'form-control'}),
        label="Source Database"
    )
    TGT_DATABASE=forms.ChoiceField(
        choices=[],
        widget=forms.Select(attrs={'class':'form-control'}),
        label="Target Database"
    )
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        sources = source_info.objects.values_list('SOURCE_ID', 'SOURCE_NM')
        self.fields['DATASRC_ID'].choices = [('', 'Select Source ID')] + list(sources)

        db_types = database_cred.objects.values_list('db_type', flat=True).distinct()
        choices = [('', 'Select Database')] + [(db, db) for db in db_types]
        self.fields['SRC_DATABASE'].choices = choices
        self.fields['TGT_DATABASE'].choices = choices       

    class Meta:
        model = srctbl_info
        fields = '__all__'
        widgets = {
            
            'SRC_SCHEMA': forms.TextInput(attrs={'class': 'form-control'}),
            'SRC_TABLENAME': forms.TextInput(attrs={'class': 'form-control'}),
            'TGT_SCHEMA': forms.TextInput(attrs={'class': 'form-control'}),
            'TGT_TABLENAME': forms.TextInput(attrs={'class': 'form-control'}),
            'SELECT_QUERY':forms.TextInput(attrs={'class': 'form-control'}),
            'FILTER_CON':forms.TextInput(attrs={'class': 'form-control'}),
            'RECON_REQ':forms.TextInput(attrs={'class': 'form-control'}),
            'TRG_TABLE_SFX':forms.TextInput(attrs={'class': 'form-control'}),
            'LOAD_TYPE':forms.TextInput(attrs={'class': 'form-control'}),
        }

class SourceFileInfoForm(forms.ModelForm):
    class Meta:
        model = srcfile_info
        fields = '__all__'
        widgets = {
            'DATASRC_ID': forms.NumberInput(attrs={'class': 'form-control'}),
            'SRC_DIRECTORY': forms.TextInput(attrs={'class': 'form-control'}),
            'SRC_FILENAME': forms.TextInput(attrs={'class': 'form-control'}),
            'TGT_DATABASE': forms.TextInput(attrs={'class': 'form-control'}),
            'TGT_TABLENAME': forms.TextInput(attrs={'class': 'form-control'}),
            'SRCLAYOUT_ID': forms.NumberInput(attrs={'class': 'form-control'}),
            'TRGLAYOUT_ID': forms.NumberInput(attrs={'class': 'form-control'}),
        }

class TableSchemaForm(forms.ModelForm):
    class Meta:
        model = table_schema
        fields = '__all__'
        widgets = {
            'SRCTBL_ID': forms.NumberInput(attrs={'class': 'form-control'}),
            'COLUMN_SEQ': forms.NumberInput(attrs={'class': 'form-control'}),
            'SRC_TRG_IND': forms.TextInput(attrs={'class': 'form-control'}),
            'SCHEMA_NM': forms.TextInput(attrs={'class': 'form-control'}),
            'TABLE_NM': forms.TextInput(attrs={'class': 'form-control'}),
            'COLUMN_NM': forms.TextInput(attrs={'class': 'form-control'}),
            'DATA_TYPE': forms.TextInput(attrs={'class': 'form-control'}),
            'XFRMTN': forms.TextInput(attrs={'class': 'form-control'}),
            'PRIMARY_KEY': forms.TextInput(attrs={'class': 'form-control'}),
            'DEFAULT_VAL': forms.TextInput(attrs={'class': 'form-control'}),
        }

class DatabaseCredForm(forms.ModelForm):
    class Meta:
        model = database_cred
        fields = '__all__'
        widgets = {
            'db_type': forms.TextInput(attrs={'class': 'form-control'}),
            'db_role': forms.TextInput(attrs={'class': 'form-control'}),
            'host': forms.TextInput(attrs={'class': 'form-control'}),
            'port': forms.NumberInput(attrs={'class': 'form-control'}),
            'database': forms.TextInput(attrs={'class': 'form-control'}),
            'username': forms.TextInput(attrs={'class': 'form-control'}),
            'password': forms.PasswordInput(attrs={'class': 'form-control'}),
        }

class ExecutionTrackForm(forms.ModelForm):
    class Meta:
        model = execution_track
        fields = '__all__'
        widgets = {
            'EXECUTION_DT': forms.DateInput(attrs={'class': 'form-control', 'type': 'date'}),
            'SRCTBL_ID': forms.NumberInput(attrs={'class': 'form-control'}),
            'COMPLETE_TRACK': forms.TextInput(attrs={'class': 'form-control', 'maxlength': 1}),
            'REC_LOAD_COUNT': forms.NumberInput(attrs={'class': 'form-control'}),
            'LAST_EXEC_DT': forms.DateInput(attrs={'class': 'form-control', 'type': 'date'}),
            'NEXT_EXEC_DT': forms.DateInput(attrs={'class': 'form-control', 'type': 'date'}),
        }