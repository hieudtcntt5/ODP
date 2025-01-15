#!/usr/bin/env python3
import sys, os, pwd, grp, signal, time, base64
from resource_management import *
from resource_management.core.exceptions import Fail
from resource_management.core.logger import Logger
from resource_management.core.resources.system import Execute, Directory, File
from resource_management.core.shell import call
from resource_management.core.system import System
from resource_management.libraries.functions.default import default
# file airflow webserver service
def airflow_make_systemd_scripts_webserver(env):
	import params
	env.set_params(params)

	confFileText = format("""[Unit]
Description=Airflow webserver daemon
After=network.target postgresql.service mysql.service redis.service rabbitmq-server.service
Wants=postgresql.service mysql.service redis.service rabbitmq-server.service

[Service]
EnvironmentFile=/etc/sysconfig/airflow
User={airflow_user}
Group={airflow_group}
Type=simple
ExecStart={airflow_home}/airflow_control.sh webserver
Restart=on-failure
RestartSec=5s
PrivateTmp=true

[Install]
WantedBy=multi-user.target

""")

	with open("/usr/lib/systemd/system/airflow-webserver.service", 'w') as configFile:
		configFile.write(confFileText)
	configFile.close()

	confFileText = format("AIRFLOW_HOME={airflow_home}")

	with open("/etc/sysconfig/airflow", 'w') as configFile:
		configFile.write(confFileText)
	configFile.close()

	Execute("systemctl daemon-reload")
#file airflow scheduler service
def airflow_make_systemd_scripts_scheduler(env):
	import params
	env.set_params(params)

	confFileText = format("""[Unit]
Description=Airflow scheduler daemon
After=network.target postgresql.service mysql.service redis.service rabbitmq-server.service
Wants=postgresql.service mysql.service redis.service rabbitmq-server.service

[Service]
EnvironmentFile=/etc/sysconfig/airflow
User={airflow_user}
Group={airflow_group}
Type=simple
ExecStart={airflow_home}/airflow_control.sh scheduler
Restart=on-failure
RestartSec=5s
PrivateTmp=true

[Install]
WantedBy=multi-user.target

""")

	with open("/usr/lib/systemd/system/airflow-scheduler.service", 'w') as configFile:
		configFile.write(confFileText)
	configFile.close()

	confFileText = format("AIRFLOW_HOME={airflow_home}")

	with open("/etc/sysconfig/airflow", 'w') as configFile:
		configFile.write(confFileText)
	configFile.close()

	Execute("systemctl daemon-reload")
# file service flower
def airflow_make_systemd_scripts_flower(env):
	import params
	env.set_params(params)

	confFileText = format("""[Unit]
Description=Airflow flower daemon
After=network.target postgresql.service mysql.service redis.service rabbitmq-server.service
Wants=postgresql.service mysql.service redis.service rabbitmq-server.service

[Service]
EnvironmentFile=/etc/sysconfig/airflow
User={airflow_user}
Group={airflow_group}
Type=simple
ExecStart={airflow_home}/airflow_flower_control.sh 
ExecStartPost=/bin/bash -c 'echo $MAINPID > {airflow_flower_pid_file}'
Restart=on-failure
RestartSec=5s
PrivateTmp=true

[Install]
WantedBy=multi-user.target

""")

	with open("/usr/lib/systemd/system/airflow-flower.service", 'w') as configFile:
		configFile.write(confFileText)
	configFile.close()

	confFileText = format("AIRFLOW_HOME={airflow_home}")

	with open("/etc/sysconfig/airflow", 'w') as configFile:
		configFile.write(confFileText)
	configFile.close()

	Execute("systemctl daemon-reload")

def airflow_make_systemd_scripts_worker(env):
	import params
	env.set_params(params)

	confFileText = format("""[Unit]
Description=Airflow worker daemon
After=network.target postgresql.service mysql.service redis.service rabbitmq-server.service
Wants=postgresql.service mysql.service redis.service rabbitmq-server.service

[Service]
EnvironmentFile=/etc/sysconfig/airflow
User={airflow_user}
Group={airflow_group}
Type=simple
ExecStart={airflow_home}/airflow_worker_control.sh 
Restart=on-failure
RestartSec=5s
PrivateTmp=true

[Install]
WantedBy=multi-user.target

""")

	with open("/usr/lib/systemd/system/airflow-worker.service", 'w') as configFile:
		configFile.write(confFileText)
	configFile.close()

	confFileText = format("AIRFLOW_HOME={airflow_home}")

	with open("/etc/sysconfig/airflow", 'w') as configFile:
		configFile.write(confFileText)
	configFile.close()

	Execute("systemctl daemon-reload")
#script run webserver and scheduler
def airflow_make_startup_script(env):
	import params
	env.set_params(params)

	confFileText = format("""#!/bin/bash

export AIRFLOW_HOME={airflow_home}/ && source {airflow_home}/airflow_env/bin/activate && {airflow_home}/airflow_env/bin/airflow $1 --pid {airflow_home}/airflow-$1.pid && airflow triggerer
""")

	with open(format("{airflow_home}/airflow_control.sh"), 'w') as configFile:
		configFile.write(confFileText)
	configFile.close()
	Execute(format("chmod 755 {airflow_home}/airflow_control.sh"))
#script run worker
def airflow_worker_make_startup_script(env):
	import params
	env.set_params(params)

	confFileText = format("""#!/bin/bash

export AIRFLOW_HOME={airflow_home}/ && source {airflow_home}/airflow_env/bin/activate && {airflow_home}/airflow_env/bin/airflow celery worker --pid {airflow_home}/airflow-sys-worker.pid
""")

	with open(format("{airflow_home}/airflow_worker_control.sh"), 'w') as configFile:
		configFile.write(confFileText)
	configFile.close()
	Execute(format("chmod 755 {airflow_home}/airflow_worker_control.sh"))

#script run celery flower
def airflow_flower_make_startup_script(env):
	import params
	env.set_params(params)

	confFileText = format("""#!/bin/bash

export AIRFLOW_HOME={airflow_home}/ && source {airflow_home}/airflow_env/bin/activate && {airflow_home}/airflow_env/bin/airflow celery flower 
""")

	with open(format("{airflow_home}/airflow_flower_control.sh"), 'w') as configFile:
		configFile.write(confFileText)
	configFile.close()
	Execute(format("chmod 755 {airflow_home}/airflow_flower_control.sh"))

# file service triggerer
def airflow_make_systemd_scripts_triggerer(env):
	import params
	env.set_params(params)

	confFileText = format("""[Unit]
Description=Airflow triggerer daemon
After=network.target postgresql.service mysql.service redis.service rabbitmq-server.service
Wants=postgresql.service mysql.service redis.service rabbitmq-server.service

[Service]
EnvironmentFile=/etc/sysconfig/airflow
User={airflow_user}
Group={airflow_group}
Type=simple
ExecStart={airflow_home}/airflow_triggerer_control.sh 
ExecStartPost=/bin/bash -c 'echo $MAINPID > {airflow_triggerer_pid_file}'
Restart=on-failure
RestartSec=5s
PrivateTmp=true

[Install]
WantedBy=multi-user.target

""")

	with open("/usr/lib/systemd/system/airflow-triggerer.service", 'w') as configFile:
		configFile.write(confFileText)
	configFile.close()

	confFileText = format("AIRFLOW_HOME={airflow_home}")

	with open("/etc/sysconfig/airflow", 'w') as configFile:
		configFile.write(confFileText)
	configFile.close()

	Execute("systemctl daemon-reload")

#script run triggerer
def airflow_triggerer_make_startup_script(env):
	import params
	env.set_params(params)

	confFileText = format("""#!/bin/bash

export AIRFLOW_HOME={airflow_home}/ && source {airflow_home}/airflow_env/bin/activate && {airflow_home}/airflow_env/bin/airflow triggerer
""")

	with open(format("{airflow_home}/airflow_triggerer_control.sh"), 'w') as configFile:
		configFile.write(confFileText)
	configFile.close()
	Execute(format("chmod 755 {airflow_home}/airflow_triggerer_control.sh"))


#script config 
def airflow_generate_config_for_section(sections):
	"""
	Generating values for airflow.cfg for each section.
	This allows to add custom-site configuration from ambari to cfg file.
	"""
	result = {}
	for section, data in sections.items():
		section_config = ""
		for key, value in data.items():
			section_config += format("{key} = {value}\n")
		result[section] = section_config
	return result

def airflow_configure(env):
	import params
	env.set_params(params)

	airflow_config_file = ""

	airflow_config = airflow_generate_config_for_section({
		
		"database":params.config['configurations']['airflow-database-site'],
		"logging" : params.config['configurations']['airflow-logging-site'],
		"core" : params.config['configurations']['airflow-core-site'],
		"cli" : params.config['configurations']['airflow-cli-site'],
		"api" : params.config['configurations']['airflow-api-site'],
		"operators" : params.config['configurations']['airflow-operators-site'],
		"webserver" : params.config['configurations']['airflow-webserver-site'],
		"email" : params.config['configurations']['airflow-email-site'],
		"smtp" : params.config['configurations']['airflow-smtp-site'],
		"celery" : params.config['configurations']['airflow-celery-site'],
		"dask" : params.config['configurations']['airflow-dask-site'],
		"scheduler" : params.config['configurations']['airflow-scheduler-site'],
		"ldap" : params.config['configurations']['airflow-ldap-site'],
		"mesos" : params.config['configurations']['airflow-mesos-site'],
		"kerberos" : params.config['configurations']['airflow-kerberos-site'],
		"github_enterprise" : params.config['configurations']['airflow-githubenterprise-site'],
		"admin" : params.config['configurations']['airflow-admin-site'],
		"lineage" : params.config['configurations']['airflow-lineage-site'],
		"atlas" : params.config['configurations']['airflow-atlas-site'],
		"hive" : params.config['configurations']['airflow-hive-site'],
		"celery_broker_transport_options" : params.config['configurations']['airflow-celery_broker_transport_options-site'],
		"elasticsearch" : params.config['configurations']['airflow-elasticsearch-site'],
		"kubernetes" : params.config['configurations']['airflow-kubernetes-site'],
	})

	for section, value in airflow_config.items():
		airflow_config_file += format("[{section}]\n{value}\n")

	with open(params.airflow_home + "/airflow.cfg", 'w') as configFile:
		configFile.write(airflow_config_file)
	configFile.close()
