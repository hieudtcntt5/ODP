import sys, os, pwd, grp, signal, time
from resource_management import *
from subprocess import call
from airflow_setup import *

class AirflowScheduler(Script):
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
		Execute(format("/usr/bin/yum localinstall -y {service_packagedir}/scripts/rpm/python3-rpm-generators*"))
		Execute(format("/usr/bin/yum localinstall -y {service_packagedir}/scripts/rpm/python3-rpm-macros*"))
		Execute(format("/usr/bin/yum localinstall -y {service_packagedir}/scripts/rpm/*"), ignore_failures=True)

		self.install_packages(env)
		Logger.info(format("Installing Airflow Service"))
		Execute(format("pip3.9 install {service_packagedir}/scripts/virtualenv/*"))
		Execute(format("/usr/local/bin/virtualenv {airflow_home}/airflow_env --python=python3.9"))
		Execute(format("{airflow_home}/airflow_env/bin/pip3.9 install  {service_packagedir}/scripts/all/* "))
		Execute(format("export SLUGIFY_USES_TEXT_UNIDECODE=yes && {airflow_home}/airflow_env/bin/pip3.9 install apache-airflow"))
		Execute(format("export SLUGIFY_USES_TEXT_UNIDECODE=yes && {airflow_home}/airflow_env/bin/pip3.9 install apache-airflow[celery]"))
		Execute(format("export SLUGIFY_USES_TEXT_UNIDECODE=yes && {airflow_home}/airflow_env/bin/pip3.9 install openmetadata-managed-apis"))
		Execute(format("chmod 755 {airflow_home}/airflow_env/bin/airflow"))
		Execute(format("useradd {airflow_user}"), ignore_failures=True)
		Execute(format("mkdir -p {airflow_home}"))
		airflow_make_startup_script(env)
		Execute(format("chown -R {airflow_user}:{airflow_group} {airflow_home} "))
		airflow_configure(env)
		Execute(format("export AIRFLOW_HOME={airflow_home} && {airflow_home}/airflow_env/bin/airflow db init"),
			user=params.airflow_user
		)
		Execute(format("export AIRFLOW_HOME={airflow_home} && {airflow_home}/airflow_env/bin/airflow users create --username admin -p admin --firstname admin --lastname devadmin --role Admin --email admin@example.org"),
			user=params.airflow_user
		)
	def configure(self, env):
		import params
		env.set_params(params)
		airflow_configure(env)
		airflow_make_systemd_scripts_scheduler(env)
		
	def start(self, env):
		import params
		self.configure(env)
		Execute("service airflow-scheduler start")
		Execute('ps -ef | grep "airflow scheduler" | grep -v grep | awk \'{print $2}\' | tail -n 1 > ' + params.airflow_scheduler_pid_file, 
			user=params.airflow_user
		)

	def stop(self, env):
		import params
		env.set_params(params)
		# Kill the process of Airflow
		Execute("service airflow-scheduler stop")
		File(params.airflow_scheduler_pid_file,
			action = "delete",
			owner = params.airflow_user
		)

	def status(self, env):
		import status_params
		env.set_params(status_params)
		#use built-in method to check status using pidfile
		check_process_status(status_params.airflow_scheduler_pid_file)

if __name__ == "__main__":
	AirflowScheduler().execute()
