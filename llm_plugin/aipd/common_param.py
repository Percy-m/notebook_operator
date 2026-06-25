import os


class AipdAirflowCommonParam:
    def __init__(self):
        self.aipd_his_config = {
            "app_id": os.getenv("AIPD_APP_ID"),
            "app_key": os.getenv("AIPD_APP_KEY"),
        }


class AipdAirflowWorkflowCommonParam:
    def __init__(self, **context):
        dag_run_conf = getattr(context.get("dag_run"), "conf", {}) or {}
        self.user_id = context.get("user_id") or dag_run_conf.get("user_id")
        self.project_id = context.get("project_id") or dag_run_conf.get("project_id")
