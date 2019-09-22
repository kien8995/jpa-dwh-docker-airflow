import os
import re

from datetime import datetime
from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from airflow.operators.sensors import BaseSensorOperator
from airflow.operators.python_operator import PythonOperator

class TextFileSensorTask(object):
    def __init__(self, dag, file_path, file_pattern):
        self.dag = dag
        self.file_path = file_path
        self.file_pattern = file_pattern

    def tasks(self):
        sensor_task = OmegaFileSensor(
            task_id='file_sensor_task',
            filepath=self.file_path,
            filepattern=self.file_pattern,
            poke_interval=3,
            mode='reschedule',
            dag=self.dag
        )

        proccess_task = PythonOperator(
            task_id='process_the_file', 
            python_callable=self.__process_file,
            provide_context=True,
            dag=self.dag
        )

        sensor_task >> proccess_task
        return sensor_task, proccess_task

    def __process_file(self, **context):
        file_to_process = context['task_instance'].xcom_pull(key='file_name', task_ids='file_sensor_task')
        file = open(self.file_path + file_to_process, 'w')
        file.write('This is a testttttttttttttttt\n')
        file.close()

class ArchiveFileOperator(BaseOperator):
    @apply_defaults
    def __init__(self, filepath, archivepath, *args, **kwargs):
        super(ArchiveFileOperator, self).__init__(*args, **kwargs)
        self.filepath = filepath
        self.archivepath = archivepath

    def execute(self, context):
        file_name = context['task_instance'].xcom_pull(
            'file_sensor_task', key='file_name')
        os.rename(self.filepath + file_name, self.archivepath + file_name)


class OmegaFileSensor(BaseSensorOperator):
    @apply_defaults
    def __init__(self, filepath, filepattern, *args, **kwargs):
        super(OmegaFileSensor, self).__init__(*args, **kwargs)
        self.filepath = filepath
        self.filepattern = filepattern

    def poke(self, context):
        full_path = self.filepath
        file_pattern = re.compile(self.filepattern)

        directory = os.listdir(full_path)

        for files in directory:
            if re.match(file_pattern, files):
                context['task_instance'].xcom_push('file_name', files)
                return True
        return False