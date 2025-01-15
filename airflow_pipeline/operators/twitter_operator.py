from airflow.models import BaseOperator ,DAG, TaskInstance
import sys
sys.path.append("airflow_pipeline")
import json
from hook.x_hook import TwitterHook
from datetime import datetime, timedelta
from pathlib import Path 
from os.path import join


class TwitterOperator(BaseOperator):
    template_fields = ["query", "file_path", "start_time", "end_time"]

    def __init__(self, end_time, start_time, file_path, query, **kwargs):
        self.end_time = end_time
        self.start_time = start_time
        self.query = query
        self.file_path = file_path

        super().__init__(**kwargs)


    def create_parent_folder(self):
        (Path(self.file_path).parent).mkdir(parents=True, exist_ok=True)     

    def execute(self, context):
        end_time = self.end_time
        start_time = self.start_time
        query = self.query
        self.create_parent_folder()
    

        with open(self.file_path, "w") as saida_dados:
            for pg in TwitterHook(end_time, start_time, query).run():
                json.dump(pg,saida_dados, ensure_ascii=False)
                saida_dados.write("\n")


if __name__ == "__main__":

   #montando url
    TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"

    end_time = datetime.now().strftime(TIMESTAMP_FORMAT)
    start_time = (datetime.now() +  timedelta(-1)).date().strftime(TIMESTAMP_FORMAT)   
    query = "datascience"


    with DAG(dag_id = "TwitterTest", start_date=datetime.now()):
        to = TwitterOperator(file_path=join("datalake/twitter_datascience",
                            f"extract_date={datetime.now().date()}",
                            f"datascience_{datetime.now().date().strftime('%Y%m%d')}.json"),query=query, start_time=start_time , end_time=end_time, task_id="test_run")
        ti = TaskInstance(task=to)
        to.execute(ti.task_id)