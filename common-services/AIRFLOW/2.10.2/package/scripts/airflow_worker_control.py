import sys, os, pwd, grp, signal, time
from resource_management import *
from subprocess import call
from airflow_setup import *

class AirflowWorker(Script):
	"""
	Contains the interface definitions for methods like install, 
	start, stop, status, etc. for the Airflow Server
	"""
	def install(self, env):
		# if not os.path.exists("/bin/pip3.9"):
		# 	Execute(format("ln -s /usr/local/bin/pip3.9 /bin/"))
		# if not os.path.exists("/usr/bin/pip3.9"):
		# 	Execute(format("ln -s /usr/local/bin/pip3.9 /usr/bin/"))
		import params
		env.set_params(params)
		print(env)
		Execute(format("/usr/bin/yum localinstall -y {service_packagedir}/scripts/rpm/python3-rpm-generators*"))
		Execute(format("/usr/bin/yum localinstall -y {service_packagedir}/scripts/rpm/python3-rpm-macros*"))
		Execute(format("/usr/bin/yum localinstall -y {service_packagedir}/scripts/rpm/*"), ignore_failures=True)
		self.install_packages(env)
		Logger.info(format("Installing Airflow Service"))
		Execute(format("pip3.9 install {service_packagedir}/scripts/virtualenv/*"))
		Execute(format("/usr/local/bin/virtualenv {airflow_home}/airflow_env --python=python3.9"))
		#Execute(format("{airflow_home}/airflow_env/bin/pip3.9 install {service_packagedir}/scripts/all/ "))
		Execute(format("{airflow_home}/airflow_env/bin/pip3.9 install  {service_packagedir}/scripts/all/* "))
		Execute(format("export SLUGIFY_USES_TEXT_UNIDECODE=yes && {airflow_home}/airflow_env/bin/pip3.9 install apache-airflow"))
		Execute(format("export SLUGIFY_USES_TEXT_UNIDECODE=yes && {airflow_home}/airflow_env/bin/pip3.9 install apache-airflow[celery]"))
		Execute(format("export SLUGIFY_USES_TEXT_UNIDECODE=yes && {airflow_home}/airflow_env/bin/pip3.9 install openmetadata-managed-apis"))
		Execute(format("chmod 755 {airflow_home}/airflow_env/bin/airflow"))
		Execute(format("useradd {airflow_user}"), ignore_failures=True)
		Execute(format("mkdir -p {airflow_home}"))
		Execute(format("chown -R {airflow_user}:{airflow_group} {airflow_home} "))
		airflow_configure(env)
		airflow_worker_make_startup_script(env)

		

	def configure(self, env):
		import params
		env.set_params(params)
		airflow_configure(env)
		airflow_worker_make_startup_script(env)
		airflow_make_systemd_scripts_worker(env)
		
	def start(self, env):
		import params
		self.configure(env)
		Execute("service airflow-worker stop")
		Execute("service airflow-worker start")


	def stop(self, env):
		import params
		env.set_params(params)
		# Kill the process of Airflow
		Execute("service airflow-worker stop")
		File(params.airflow_worker_pid_file,
			action = "delete",
			owner = params.airflow_user
		)

	def status(self, env):
		import status_params
		import params
		#env.set_params(status_params)
		#use built-in method to check status using pidfile
		env.set_params(params)
		check_process_status(params.airflow_worker_pid_file)

if __name__ == "__main__":
	AirflowWorker().execute()
