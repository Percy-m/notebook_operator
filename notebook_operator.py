import json
import tempfile
import os
import papermill as pm

import requests

from airflow.providers.standard.operators.python import PythonOperator

from llm_plugin.aipd.common_param import AipdAirflowCommonParam, AipdAirflowWorkflowCommonParam


class NotebookOperator(PythonOperator):

    def __init__(
            self,
            parameters: dict,
            task_id: str = 'notebook_operator',
            num_executors: int =1,
            executor_cores: int =1,
            driver_memory: int =512,
            executor_memory: int =512,
            kernel_name: str = "python3",
            **kwargs
    ):
        super().__init__(
            task_id=task_id,
            python_callable=self._execute,
            **kwargs
        )
        # 传入完整ipynb json字符串，替代原来的remote_url下载
        self.output_nb = "output_nb"
        self.parameters = parameters
        self.kernel_name = kernel_name

    def send_http_get(self, url: str, params: dict = None, headers: dict = None) -> requests.Response:
        try:
            response = requests.get(url, params=params, headers=headers)
            self.log.info(f"HTTP请求响应状态码: {response.status_code}")
            return response
        except Exception as e:
            self.log.error(f"HTTP请求失败: {str(e)}", exc_info=True)
            raise

    def send_http_post(self, url: str, file_path: str, data: dict, headers: dict = None) -> requests.Response:
        try:
            with open(file_path, "rb") as f:
                files = {"file": (os.path.basename(file_path), f)}
                response = requests.post(url, files=files, data=data, headers=headers)
            self.log.info(f"HTTP请求响应状态码: {response.status_code}")
            self.log.info(f"HTTP请求响应内容: {response.text}")
            return response
        except Exception as e:
            self.log.error(f"HTTP请求失败: {str(e)}", exc_info=True)
            raise

    def _execute(self, **context):
        common_param = AipdAirflowCommonParam()
        workflow_param = AipdAirflowWorkflowCommonParam(**context)

        # 用户和项目信息
        user_id = workflow_param.user_id
        project_id = workflow_param.project_id

        print(f"appid: {common_param.aipd_his_config.get('app_id')}")
        headers = {
            "X-HW-ID": common_param.aipd_his_config.get("app_id"),
            "X-HW-APPKEY": common_param.aipd_his_config.get("app_key")
        }

        params = {"workspace_uuid": "00-532K0135V500000000000000137U9W68PKMWW", "path": "/123456.ipynb"}
        response = self.send_http_get("https://apigw-beta.huawei.com/api/alpha/workspacecore/v1/object/get", params, headers)

        data = json.loads(response.content)

        content = data["data"]["content"]

        # 创建临时ipynb文件，delete=False 手动控制删除
        tmp_fd, tmp_nb_path = tempfile.mkstemp(suffix=".ipynb")
        os.close(tmp_fd)
        self.log.info(f"创建临时Notebook文件：{tmp_nb_path}")

        try:
            # 将传入的ipynb字符串写入本地临时文件
            with open(tmp_nb_path, "w", encoding="utf-8") as f:
                f.write(content)
            self.log.info(f"写入Notebook内容完成，长度：{len(content)}")

            # Papermill同步执行Spark Notebook
            pm.execute_notebook(
                input_path=tmp_nb_path,
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
                self.log.info(f"========== output_nb 文件内容 ==========\n{output_content}\n========== 输出结束 ==========")

        except Exception as e:
            self.log.error(f"Notebook执行失败: {str(e)}", exc_info=True)
            raise
        finally:
            # 无论成功失败，清理临时文件
            if os.path.exists(tmp_nb_path):
                os.unlink(tmp_nb_path)
                self.log.info(f"临时文件已清理：{tmp_nb_path}")

# {task_name} = NotebookOperator(
#     parameters="{parameters}",
#     task_id="{task_name}",
#     num_executors="{numExecutors}",
#     executor_cores="{executorCores}",
#     driver_memory="{driverMemory}",
#     executor_memory="{executorMemory}"
# )
