# Generated by Django 4.0.6 on 2022-09-29 04:27

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('aksara', '0006_kkmnowjson_chart_name'),
    ]

    operations = [
        migrations.AddField(
            model_name='kkmnowjson',
            name='chart_type',
            field=models.CharField(max_length=200, null=True),
        ),
    ]