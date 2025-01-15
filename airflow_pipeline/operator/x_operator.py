from airflow import BaseOperator
import json
from hook.x_hook import TwitterHook
from datetime import datetime, timedelta


class TwitterOperator(BaseOperator):

    def __init__(self, end_time, start_time, query, **kwargs):
        self.end_time = end_time
        self.start_time = start_time
        self.query = query

        super().__init__(**kwargs)

    def execute(self, context):
        end_time = self.end_time
        start_time = self.start_time
        query = self.query
    

        with open("extract_x.json" , "w") as saida_dados:
            for pg in TwitterHook(end_time, start_time, query).run():
                json.dump(pg,saida_dados, ensure_ascii=False)
                saida_dados.write("\n")


if __name__ == "__main__":

   #montando url
    TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"

    end_time = datetime.now().strftime(TIMESTAMP_FORMAT)
    start_time = (datetime.now() +  timedelta(-1)).date().strftime(TIMESTAMP_FORMAT)   
    query = "datascience"