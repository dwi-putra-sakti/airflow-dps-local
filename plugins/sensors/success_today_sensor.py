from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from airflow.models import DagRun
from airflow.utils.state import DagRunState
from datetime import datetime
import pendulum

class DagSuccessTodaySensor(BaseSensorOperator):
    @apply_defaults
    def __init__(self, external_dag_id, timezone='Asia/Jakarta', *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.external_dag_id = external_dag_id
        self.timezone = pendulum.timezone(timezone)

    # def poke(self, context):
    #     today = datetime.now(self.timezone).date()
    #     runs = DagRun.find(dag_id=self.external_dag_id, state=DagRunState.SUCCESS)

    #     for run in runs:
    #         if run.execution_date.astimezone(self.timezone).date() == today:
    #             self.log.info(f"DAG {self.external_dag_id} sudah sukses hari ini.")
    #             return True

    #     self.log.info(f"Belum ada DAG {self.external_dag_id} yang sukses hari ini.")
    #     return False
    

    def poke(self, context):
        now = datetime.now(self.timezone)
        twenty_four_hours_ago = now - pendulum.duration(hours=24)
        
        runs = DagRun.find(dag_id=self.external_dag_id, state=DagRunState.SUCCESS)

        for run in runs:
            run_time = run.execution_date.astimezone(self.timezone)
            if run_time >= twenty_four_hours_ago:
                self.log.info(
                    f"DAG {self.external_dag_id} sukses dalam 24 jam terakhir pada {run_time}."
                )
                return True

        self.log.info(
            f"Belum ada DAG {self.external_dag_id} yang sukses dalam 24 jam terakhir."
        )
        return False

