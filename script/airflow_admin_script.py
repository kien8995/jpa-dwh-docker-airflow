import os
import airflow
from airflow import models, settings
from airflow.contrib.auth.backends.password_auth import PasswordUser

user = PasswordUser(models.User())
user.username = os.environ['AIRFLOW_ADMIN_USER']
user.email = os.environ['AIRFLOW_ADMIN_EMAIL']
user.password = os.environ['AIRFLOW_ADMIN_PASS']
user.superuser = True
session = settings.Session()

session.add(user)
session.commit()
session.close()
