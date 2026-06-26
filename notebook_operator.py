import json
import tempfile
import os
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
        },
        "gamma": {
            "get_user_info": "https://apigw-beta.huawei.com/api/gamma/bizadmin/user/v1/search/batch/get",
            "get_workspace_object": "https://apigw-beta.huawei.com/api/gamma/workspacecore/v1/object/get",
            "get_tenant_binding": "https://apigw-beta.huawei.com/api/gamma/bizadmin/project/v1/tenant-binding/get",
            "get_dag_info": "https://apigw-beta.huawei.com/api/gamma/workspacecore/api/v1/internal/dag/get",
            "get_dag_run_info": "https://apigw-beta.huawei.com/api/gamma/workspacecore/api/v1/wfapi/dagrun/query",
        },
        "prod": {
            "get_user_info": "https://apigw.huawei.com/api/bizadmin/user/v1/search/batch/get",
            "get_workspace_object": "https://apigw.huawei.com/api/workspacecore/v1/object/get",
            "get_tenant_binding": "https://apigw.huawei.com/api/bizadmin/project/v1/tenant-binding/get",
            "get_dag_info": "https://apigw.huawei.com/api/workspacecore/api/v1/internal/dag/get",
            "get_dag_run_info": "https://apigw.huawei.com/api/workspacecore/api/v1/wfapi/dagrun/query",
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

            if path.endswith(".ipynb"):
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
