import json
import tempfile
import os
import time
import papermill as pm

import requests

from airflow.providers.standard.operators.python import PythonOperator



class NotebookOperator(PythonOperator):

    # 不同环境下远程接口地址映射
    REMOTE_API_MAP = {
        "dev": {
            "get_user_info": "https://apigw-beta.huawei.com/api/alpha/bizadmin/user/v1/search/batch/get",
            "get_workspace_object": "https://apigw-beta.huawei.com/api/alpha/workspacecore/v1/object/get",
            "get_tenant_binding": "https://apigw-beta.huawei.com/api/alpha/bizadmin/project/v1/tenant-binding/get",
            "get_dag_info": "https://apigw-beta.huawei.com/api/alpha/workspacecore/api/v1/internal/dag/get",
            "get_dag_run_info": "https://apigw-beta.huawei.com/api/alpha/workspacecore/api/v1/wfapi/dagrun/query",
            "create_spark_job": "https://apigw-beta.huawei.com/api/alpha/compRoute/job/create",
            "get_spark_job": "https://apigw-beta.huawei.com/api/alpha/compRoute/job/get",
        },
        "gamma": {
            "get_user_info": "https://apigw-beta.huawei.com/api/gamma/bizadmin/user/v1/search/batch/get",
            "get_workspace_object": "https://apigw-beta.huawei.com/api/gamma/workspacecore/v1/object/get",
            "get_tenant_binding": "https://apigw-beta.huawei.com/api/gamma/bizadmin/project/v1/tenant-binding/get",
            "get_dag_info": "https://apigw-beta.huawei.com/api/gamma/workspacecore/api/v1/internal/dag/get",
            "get_dag_run_info": "https://apigw-beta.huawei.com/api/gamma/workspacecore/api/v1/wfapi/dagrun/query",
            "create_spark_job": "https://apigw-beta.huawei.com/api/gamma/compRoute/job/create",
            "get_spark_job": "https://apigw-beta.huawei.com/api/gamma/compRoute/job/get",
        },
        "prod": {
            "get_user_info": "https://apigw.huawei.com/api/bizadmin/user/v1/search/batch/get",
            "get_workspace_object": "https://apigw.huawei.com/api/workspacecore/v1/object/get",
            "get_tenant_binding": "https://apigw.huawei.com/api/bizadmin/project/v1/tenant-binding/get",
            "get_dag_info": "https://apigw.huawei.com/api/workspacecore/api/v1/internal/dag/get",
            "get_dag_run_info": "https://apigw.huawei.com/api/workspacecore/api/v1/wfapi/dagrun/query",
            "create_spark_job": "https://apigw.huawei.com/api/compRoute/job/create",
            "get_spark_job": "https://apigw.huawei.com/api/compRoute/job/get",
        },
    }

    def __init__(
            self,
            params: dict = None,
            task_id: str = 'notebook_operator',
            kernel_name: str = "python3",
            **kwargs
    ):

        if params is None:
            params = {}

        kwargs['params'] = params

        super().__init__(
            task_id=task_id,
            python_callable=self._execute,
            **kwargs
        )

        self.output_nb = "output_nb"
        self.parameters = json.loads(params["parameters"])
        self.params = params
        self.kernel_name = kernel_name
        self.env = os.environ.get("AIPD_REGION", "dev") or "dev"
        self.user_info = {}

    def get_user_info(self, user_id: str, headers: dict = None) -> dict:
        url = self.REMOTE_API_MAP[self.env]["get_user_info"]
        resp = self.send_http_get(url, data={"uids": [user_id], "queryType": "number"}, headers=headers)
        data = json.loads(resp.content)
        self.user_info = data["data"]["data"][0]
        self.log.info(f"global_user_id: {self.user_info['global_user_id']}")

    # 查询connect
    def get_tenant_binding(self, project_id: str, headers: dict = None) -> dict:
        url = self.REMOTE_API_MAP[self.env]["get_tenant_binding"]
        json_data = {
            "devucProjectId": project_id,
            "globalUserId": self.user_info['global_user_id']
        }
        resp = self.send_http_post(url, json_data=json_data, headers=headers)
        data = json.loads(resp.content)
        return data["data"]

    def get_dag_info(self, headers: dict = None) -> dict:
        dag_id = os.environ.get('AIRFLOW_CTX_DAG_ID', '')
        self.log.info(f"dag_id: {dag_id}")
        url = self.REMOTE_API_MAP[self.env]["get_dag_info"]
        json_data = {"dagId": dag_id}
        resp = self.send_http_post(url, json_data=json_data, headers=headers)
        data = json.loads(resp.content)
        return data["data"]

    def get_dag_run_info(self, json_data: dict = None, headers: dict = None) -> dict:
        url = self.REMOTE_API_MAP[self.env]["get_dag_run_info"]
        resp = self.send_http_post(url, json_data=json_data, headers=headers)
        data = json.loads(resp.content)
        return data["data"]

    def send_http_get(self, url: str, data: dict = None, headers: dict = None) -> requests.Response:
        try:
            response = requests.get(url, params=data, headers=headers)
            self.log.info(f"HTTP请求响应状态码: {response.status_code}")
            return response
        except Exception as e:
            self.log.error(f"HTTP请求失败: {str(e)}", exc_info=True)
            raise

    def send_http_post(self, url: str, file_path: str = None, data: dict = None, json_data: dict = None, headers: dict = None) -> requests.Response:
        try:
            if file_path is not None:
                # 文件上传请求
                with open(file_path, "rb") as f:
                    files = {"file": (os.path.basename(file_path), f)}
                    response = requests.post(url, files=files, data=data, json=json_data, headers=headers)
            elif json_data is not None:
                # 纯 JSON 请求
                response = requests.post(url, json=json_data, headers=headers)
            else:
                # 纯表单数据请求
                response = requests.post(url, data=data, headers=headers)
            self.log.info(f"HTTP请求响应状态码: {response.status_code}")
            self.log.info(f"HTTP请求响应内容: {response.text}")
            return response
        except Exception as e:
            self.log.error(f"HTTP请求失败: {str(e)}", exc_info=True)
            raise

    def get_resource_id(self, project_id: str, headers: dict = None) -> str:
        required_resource = self.calculate_task_resource()
        resource_pools = self.query_resource_pools(project_id, headers=headers)
        selected_pool = self.select_resource_pool(resource_pools, required_resource)
        resource_id = selected_pool["poolUid"]
        self.log.info(
            f"选择资源池: {resource_id}, "
            f"required_cpu: {required_resource['cpu']}, "
            f"required_memory: {required_resource['memory']}"
        )
        return resource_id

    def calculate_task_resource(self) -> dict:
        driver_cores = 1
        driver_memory_overhead = 0
        num_executors = int(self.params.get("num_executors", 1))
        executor_cores = int(self.params.get("executor_cores", 1))
        driver_memory = self.parse_resource_value(self.params.get("driver_memory", 512))
        executor_memory = self.parse_resource_value(self.params.get("executor_memory", 512))
        cpu = driver_cores + num_executors * executor_cores
        memory = driver_memory + driver_memory_overhead + num_executors * executor_memory
        return {
            "cpu": cpu,
            "memory": memory
        }

    def query_resource_pools(self, project_id: str, headers: dict = None) -> list:
        url = self.build_resource_pool_query_url()
        response = self.send_http_get(url, headers=headers)
        data = json.loads(response.content)
        if data.get("status") != "OK":
            raise RuntimeError(f"资源池列表查询失败: {data}")
        resource_pools = data.get("data") or []
        if not isinstance(resource_pools, list):
            raise ValueError(f"资源池列表响应格式错误: {data}")
        return resource_pools

    def build_resource_pool_query_url(self) -> str:
        resource_type = self.params.get("resource_type")
        if not resource_type:
            raise ValueError("resource_type is required to query resource pools")
        comp_route_base = self.REMOTE_API_MAP[self.env]["create_spark_job"].rsplit("/job/create", 1)[0]
        return f"{comp_route_base}/{str(resource_type).strip('/')}/queryAll"

    def select_resource_pool(self, resource_pools: list, required_resource: dict) -> dict:
        required_cpu = required_resource["cpu"]
        required_memory = required_resource["memory"]
        for resource_pool in resource_pools:
            remaining_cpu = self.parse_resource_value(resource_pool.get("remainingCpuQuota", 0))
            remaining_memory = self.parse_resource_value(resource_pool.get("remainingMemoryQuota", 0))
            if remaining_cpu >= required_cpu and remaining_memory >= required_memory:
                if not resource_pool.get("poolUid"):
                    raise ValueError(f"资源池缺少poolUid: {resource_pool}")
                return resource_pool
        raise RuntimeError(
            f"没有满足资源需求的资源池，required_cpu: {required_cpu}, "
            f"required_memory: {required_memory}"
        )

    def build_spark_job_params(self, file_name: str) -> dict:
        return {
            "numExecutors": self.params.get("num_executors", 1),
            "executorCores": self.params.get("executor_cores", 1),
            "driverMemory": self.format_memory(self.params.get("driver_memory", 512)),
            "executorMemory": self.format_memory(self.params.get("executor_memory", 512)),
            "mainClass": file_name,
            "mainClassParameter": json.dumps(self.parameters, ensure_ascii=False),
            "appJar": file_name,
            "dbName": "default"
        }

    def build_spark_create_job_vo(self, file_name: str, project_id: str, headers: dict = None) -> dict:
        tenant_binding = self.get_tenant_binding(project_id, headers=headers)
        connection_id = self.extract_first_value(tenant_binding, "connectionId", "connection_id", "id")
        connection = self.extract_first_value(tenant_binding, "connection", "connectionName", "name")
        if not connection_id or not connection:
            raise ValueError("connectionId and connection are required from get_tenant_binding")

        return {
            "jobName": self.task_id,
            "jobParams": json.dumps(self.build_spark_job_params(file_name), ensure_ascii=False),
            "jobBizId": self.task_id,
            "projectId": project_id,
            "engineProvider": "FCS",
            "engineType": "SPARK",
            "engineJobType": "SPARK_PY",
            "resourceId": self.get_resource_id(project_id, headers=headers),
            "connectionId": connection_id,
            "connection": connection,
            "jobSource": "NOTEBOOK",
            "owner": self.user_info["global_user_id"]
        }

    def create_spark_job(self, file_path: str, file_name: str, project_id: str, headers: dict = None) -> str:
        url = self.REMOTE_API_MAP[self.env]["create_spark_job"]
        create_job_vo = self.build_spark_create_job_vo(file_name, project_id, headers=headers)
        data = {"createJobVo": json.dumps(create_job_vo, ensure_ascii=False)}
        try:
            with open(file_path, "rb") as f:
                files = {"jobFileList": (file_name, f)}
                response = requests.post(url, files=files, data=data, headers=headers)
            self.log.info(f"HTTP请求响应状态码: {response.status_code}")
            self.log.info(f"HTTP请求响应内容: {response.text}")
            job_id = self.extract_job_id(json.loads(response.content))
            self.log.info(f"Spark作业提交成功，job_id: {job_id}")
            return job_id
        except Exception as e:
            self.log.error(f"Spark作业提交失败: {str(e)}", exc_info=True)
            raise

    def get_spark_job(self, job_id: str, query_type: str, headers: dict = None, is_sync_job_from_provider: bool = False):
        url = self.REMOTE_API_MAP[self.env]["get_spark_job"]
        data = {
            "jobId": job_id,
            "queryType": query_type
        }
        if is_sync_job_from_provider:
            data["isSyncJobFromProvider"] = True
        response = self.send_http_get(url, data=data, headers=headers)
        return json.loads(response.content)

    def poll_spark_job(self, job_id: str, headers: dict = None) -> dict:
        poll_interval = int(self.params.get("spark_job_poll_interval", 30))
        timeout = int(self.params.get("spark_job_timeout", 3600))
        deadline = time.time() + timeout
        last_base_info = None
        last_job_log = None

        while True:
            last_base_info = self.get_spark_job(
                job_id,
                "jobBaseInfo",
                headers=headers,
                is_sync_job_from_provider=True
            )
            last_job_log = self.get_spark_job(job_id, "jobLog", headers=headers)
            self.log.info(f"Spark作业状态响应: {last_base_info}")
            self.log.info(f"Spark作业日志响应: {last_job_log}")

            job_status = self.extract_job_status(last_base_info)
            if job_status:
                normalized_status = str(job_status).lower()
                if normalized_status in {"success", "succeeded", "finished", "completed"}:
                    return {
                        "job_id": job_id,
                        "job_base_info": last_base_info,
                        "job_log": last_job_log
                    }
                if normalized_status in {"failed", "error", "cancelled", "canceled", "killed", "timeout"}:
                    raise RuntimeError(f"Spark作业执行失败，job_id: {job_id}, job_status: {job_status}")

            if time.time() >= deadline:
                raise TimeoutError(f"Spark作业轮询超时，job_id: {job_id}, timeout: {timeout}")
            time.sleep(poll_interval)

    def submit_spark_job(self, file_path: str, source_path: str, project_id: str, headers: dict = None) -> dict:
        if not source_path.endswith(".py"):
            raise ValueError("Spark作业提交仅支持 .py 文件")
        file_name = os.path.basename(source_path)
        job_id = self.create_spark_job(file_path, file_name, project_id, headers=headers)
        return self.poll_spark_job(job_id, headers=headers)

    def extract_job_id(self, data):
        if isinstance(data, str):
            return data
        if isinstance(data, dict):
            for key in ("data", "jobId", "job_id", "id"):
                value = data.get(key)
                if isinstance(value, str):
                    return value
                if value is not None:
                    nested_job_id = self.extract_job_id(value)
                    if nested_job_id:
                        return nested_job_id
        if isinstance(data, list):
            for item in data:
                nested_job_id = self.extract_job_id(item)
                if nested_job_id:
                    return nested_job_id
        raise ValueError("无法从提交响应中获取jobId")

    def extract_job_status(self, data):
        if isinstance(data, dict):
            for key in ("jobStatus", "job_status", "status"):
                if key in data:
                    return data[key]
            for value in data.values():
                nested_status = self.extract_job_status(value)
                if nested_status:
                    return nested_status
        if isinstance(data, list):
            for item in data:
                nested_status = self.extract_job_status(item)
                if nested_status:
                    return nested_status
        return None

    def extract_first_value(self, data, *keys):
        if isinstance(data, dict):
            for key in keys:
                if data.get(key):
                    return data[key]
            for value in data.values():
                nested_value = self.extract_first_value(value, *keys)
                if nested_value:
                    return nested_value
        if isinstance(data, list):
            for item in data:
                nested_value = self.extract_first_value(item, *keys)
                if nested_value:
                    return nested_value
        return None

    def format_memory(self, value) -> str:
        if isinstance(value, int):
            return f"{value}m"
        if isinstance(value, str) and value.isdigit():
            return f"{value}m"
        return str(value)

    def parse_resource_value(self, value) -> int:
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(value)
        if isinstance(value, str):
            normalized_value = value.strip().lower()
            if normalized_value.endswith("m"):
                normalized_value = normalized_value[:-1]
            if normalized_value.isdigit():
                return int(normalized_value)
        raise ValueError(f"资源数值格式错误: {value}")

    def should_submit_spark_job(self) -> bool:
        value = self.params.get("submit_spark_job", False)
        if isinstance(value, str):
            return value.lower() == "true"
        return bool(value)

    def _execute(self, **context):
        __import__('subprocess').check_call([__import__('sys').executable, '-m', 'pip', 'uninstall', 'hw-aiportal-aipd_dag_operator', '-y'])
        __import__('subprocess').check_call([__import__('sys').executable, '-m', 'pip', 'install', 'hw-aiportal-aipd_dag_operator', '--index-url', 'https://cmc.centralrepo.rnd.huawei.com/pypi/simple', '--extra-index-url', 'https://cmc.centralrepo.rnd.huawei.com/artifactory/product_pypi/simple', '--trusted-host', 'cmc.centralrepo.rnd.huawei.com'])

        # 删除主包和依赖子模块缓存
        sys_mod = __import__('sys').modules
        for mod in ("xx_operator", "llm_plugin", "llm_plugin.common_param"):
            if mod in sys_mod:
                del sys_mod[mod]

        cp = __import__("llm_plugin.aipd.common_param", fromlist=["*"])
        AipdAirflowCommonParam = cp.AipdAirflowCommonParam
        AipdAirflowWorkflowCommonParam = cp.AipdAirflowWorkflowCommonParam

        common_param = AipdAirflowCommonParam()
        workflow_param = AipdAirflowWorkflowCommonParam(**context)

        self.log.info(f"apigw_host: {common_param.his_config.get('apigw_host')}")
        self.log.info(f"appid: {common_param.apid_his_config.get('app_id')}")
        self.log.info(f"dag_run_id: {context['ti'].run_id}")
        headers = {
            "X-HW-ID": common_param.apid_his_config.get("app_id"),
            "X-HW-APPKEY": common_param.apid_his_config.get("app_key")
        }
        dag_info = self.get_dag_info(headers=headers)
        self.log.info(f"project_id: {dag_info['projectId']}")
        project_id = dag_info["projectId"]

        dag_run_info = self.get_dag_run_info({"dagId": dag_info["dagId"], "dagRunId": context['ti'].run_id}, headers=headers)
        user_id = dag_run_info["triggeringUserName"]
        self.log.info(f"user_id: {user_id}")

        path = self.params["path"] or "/hahah.ipynb"
        req_params = {"workspace_uuid": project_id, "path": path}
        response = self.send_http_get(self.REMOTE_API_MAP[self.env]["get_workspace_object"], req_params, headers)

        # 取当前用户的 global_user_id
        self.get_user_info(user_id, headers)

        data = json.loads(response.content)

        content = data["data"]["content"]

        # 根据 path 后缀名判断文件类型
        file_suffix = ".ipynb" if path.endswith(".ipynb") else ".py"
        tmp_fd, tmp_file_path = tempfile.mkstemp(suffix=file_suffix)
        os.close(tmp_fd)
        self.log.info(f"创建临时文件：{tmp_file_path}")

        try:
            # 将内容写入本地临时文件
            with open(tmp_file_path, "w", encoding="utf-8") as f:
                f.write(content)
            self.log.info(f"写入文件内容完成，长度：{len(content)}")

            if self.should_submit_spark_job():
                return self.submit_spark_job(tmp_file_path, path, project_id, headers=headers)
            elif path.endswith(".ipynb"):
                # Papermill同步执行Spark Notebook
                pm.execute_notebook(
                    input_path=tmp_file_path,
                    output_path=self.output_nb,
                    parameters=self.parameters,
                    kernel_name=self.kernel_name,
                    progress_bar=False
                )
                self.log.info(f"Notebook执行成功，输出文件：{self.output_nb}")

                # 打印output_nb文件内容到日志
                if os.path.exists(self.output_nb):
                    with open(self.output_nb, "r", encoding="utf-8") as f:
                        output_content = f.read()
                    self.log.info(
                        f"========== output_nb 文件内容 ==========\n{output_content}\n========== 输出结束 ==========")
            else:
                # 执行 .py 文件
                with open(tmp_file_path, "r", encoding="utf-8") as f:
                    py_code = f.read()
                exec_globals = {"__name__": "__main__", **self.parameters}
                exec(py_code, exec_globals)
                self.log.info(f"Python脚本执行成功")

        except Exception as e:
            self.log.error(f"文件执行失败: {str(e)}", exc_info=True)
            raise
        finally:
            # 无论成功失败，清理临时文件
            if os.path.exists(tmp_file_path):
                os.unlink(tmp_file_path)
                self.log.info(f"临时文件已清理：{tmp_file_path}")

# {task_name} = NotebookOperator(
#     task_display_name="Notebook执行",
#     task_id="{task_name}",
#     params={
#         "parameters": "{parameters}",
#         "path": "{path}",
#         "file_type": "{fileType}",
#         "resource_id": "{resourceId}",
#         "resource_type": "{resourceType}",
#         "num_executors": {numExecutors},
#         "executor_cores": {executorCores},
#         "driver_memory": {driverMemory},
#         "executor_memory": {executorMemory}
#     }
# )
