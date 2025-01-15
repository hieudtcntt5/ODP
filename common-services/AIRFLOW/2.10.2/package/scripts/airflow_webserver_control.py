import sys, os, pwd, grp, signal, time
from resource_management import *
from subprocess import call
from airflow_setup import *

class AirflowWebserver(Script):
	"""
	Contains the interface definitions for methods like install, 
	start, stop, status, etc. for the Airflow Server
	"""
	def install(self, env):
		# if not os.path.exists("/bin/pip3.9"):
		#  	Execute(format("ln -s /usr/local/bin/pip3.9 /bin/"))
		# if not os.path.exists("/usr/bin/pip3.9"):
		#  	Execute(format("ln -s /usr/local/bin/pip3.9 /usr/bin/"))
			 
		import params
		env.set_params(params)
		Execute(format("/usr/bin/yum localinstall -y {service_packagedir}/scripts/rpm/python3-rpm-generator*"))
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
		Execute(format("chown -R {airflow_user}:{airflow_group} {airflow_home} "))
		airflow_configure(env)
		airflow_make_startup_script(env)

	def configure(self, env):
		import params
		env.set_params(params)
		airflow_configure(env)
		airflow_make_systemd_scripts_webserver(env)
		
	def start(self, env):
		import params
		self.configure(env)
		Execute("service airflow-webserver start")
		#Execute(format("export AIRFLOW_HOME={airflow_home} && airflow celery flower"),
			#user=params.airflow_user)
		Execute('ps -ef | grep "airflow webserver" | grep -v grep | awk \'{print $2}\' | tail -n 1 > ' + params.airflow_webserver_pid_file,
			user=params.airflow_user
		)
		import subprocess
		# Run the command and capture the output
		process = subprocess.Popen(['journalctl', '-xe'], stdout=subprocess.PIPE)
		output, error = process.communicate()

		if isinstance(output, bytes): output = output.decode('utf-8')

		# Write the output to a file
		Execute(format("mkdir -p /home/devadmin"))
		with open('/home/devadmin/output-webserver.txt', 'w') as f:
			f.write(output)
	
	def stop(self, env):
		import params
		env.set_params(params)
		# Kill the process of Airflow
		Execute("service airflow-webserver stop")
		File(params.airflow_webserver_pid_file,
			action = "delete",
			owner = params.airflow_user
		)

	def status(self, env):
		import status_params
		env.set_params(status_params)
		#use built-in method to check status using pidfile
		check_process_status(status_params.airflow_webserver_pid_file)

	def initdb(self, env):
		import params
		env.set_params(params)
		self.configure(env)
		Execute(format("export AIRFLOW_HOME={airflow_home} && airflow db init"),
			user=params.airflow_user
		)

if __name__ == "__main__":
	AirflowWebserver().execute()
