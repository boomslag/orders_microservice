# Generated by Django 3.2.16 on 2023-04-18 23:24

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('orders', '0005_alter_orderitem_thumbnail'),
    ]

    operations = [
        migrations.AlterField(
            model_name='orderitem',
            name='thumbnail',
            field=models.CharField(blank=True, max_length=255, null=True),
        ),
    ]