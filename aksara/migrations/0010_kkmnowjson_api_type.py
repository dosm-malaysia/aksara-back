# Generated by Django 4.0.6 on 2022-09-29 06:11

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('aksara', '0009_kkmnowjson'),
    ]

    operations = [
        migrations.AddField(
            model_name='kkmnowjson',
            name='api_type',
            field=models.CharField(max_length=200, null=True),
        ),
    ]