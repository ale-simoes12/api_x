from airflow import BaseOperator
import json
from hook.x_hook import TwitterHook


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

        for pg in TwitterHook(end_time, start_time, query).run():
            print(json.dumps(pg, indent=4, sort_keys=True))

            