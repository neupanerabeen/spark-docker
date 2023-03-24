import sys
import os
import json
import docker
import logging
import requests


class CreateCluster():
	__assets_folder = "assets"
	__network_name = "cluster_network"
	__image_name="ubuntu_base:latest"
	__base_image_name="ubuntu:latest"
	
	def __init__(self, config):
		self.__all_containers = {}

		self.config = self.__get_config(config)
		self.docker_client = docker.from_env()
		# self.log = logging.basicConfig()
		
		work_dir = os.path.dirname(os.path.realpath(__file__))
		self.config["work_dir"] = work_dir 
		try:
			log_file = self.config["log_file"]
		except Exception as e:
			log_file = "{}/{}".format(self.config["work_dir"], "logs.log")

		log_level = logging.getLevelName(self.config["log_level"])
		logging.basicConfig(
			filename=log_file,
			level=log_level, 
			format= '[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s',
			datefmt='%H:%M:%S'
		)
		try:
			os.mkdir("assets")
		except FileExistsError as _fe:
			pass



	def __get_config(self, config_file):
		default_config = {
			"number_of_workers":1,
			"memory_limit":"1g",
			"disk_limit":"1g",
			"log_level":"INFO",
		}

		try:
			with open(config_file, 'r') as _config_file:
				user_config = json.loads(_config_file.read())
		except FileNotFoundError as _fe:
			print("Config file not found:{}".format(config_file))
			exit(-1)
		except json.decoder.JSONDecodeError as js_err:
			print("The config file is not a valid json file")
			exit(-1)
		config = {**default_config, **user_config}
		work_dir = os.path.dirname(os.path.realpath(__file__))
		config["work_dir"] = work_dir 
		return config


	def __get_spark(self):
		max_retries = 3
		counter = 0
		logging.info("downloading data")
		while(counter < max_retries):
			try:
				with requests.get("https://dlcdn.apache.org/spark/spark-3.2.1/spark-3.2.1-bin-hadoop3.2.tgz") as sp_file:
					with open("assets/spark.tgz", "wb") as sp_writer:
						sp_writer.write(sp_file.content)
						break
			except Exception as e:
				counter+=1
				logging.warning("Cannot download spark")
				logging.warning(e)


	def prepare_image(self):
		logging.info("Pulling ubuntu:latest image")
		self.docker_client.images.pull("ubuntu:latest")
		container = self.docker_client.containers.run(
			image=self.__base_image_name,
			detach=True,
			hostname="base",
			name="master1",
			tty=True,
			mem_limit=self.config["memory_limit"],
			network=self.__network_name,
			ports={
				8080:8080,
				# 50070:50070,
			},
			environment={
				"BI2I_CLUSTER_TYPE":"MASTER"
			},
			volumes={ self.config["work_dir"]+"/assets": {
						'bind': '/opt/assets', 
						'mode': 'rw'
				},
			},
			# command="service ssh start &&   cp $SPARK_HOME/conf/slaves.template $SPARK_HOME/config/slaves &&  cat /opt/assets/workers.txt > $SPARK_HOME/conf/slaves && $SPARK_HOME/sbin/start-all.sh"
		)
		logging.info("PULLED ubuntu:latest image...complete")
		

	def prepare_assets(self):
		# self.__get_spark()
		# self.prepare_image()
		...

	
	def prepare_scripts(self):
		with open(self.config["work_dir"]+"/assets/workers.txt", 'w') as _worker_file:
			_worker_file.write("master1\n")
			for worker_id in range(0,int(self.config["number_of_workers"])):
				_worker_file.write("worker{}\n".format(worker_id+1))

	def prepare_network(self):
		logging.info("Fetching if network with name:{} already exists".format(self.__network_name))
		networks = self.docker_client.networks.list(names = self.__network_name)
		logging.warning("Network with same name:{} already exists. Attempting to remove them".format(self.__network_name))

		for network in networks:
			short_id = network.short_id
			logging.info("Removing network with id:{}".format(short_id))
			network.remove()

		self.docker_client.networks.create(self.__network_name, driver="bridge")

	def prepare_container(self):
		logging.info("Preparing containers")
		existing_containers = self.docker_client.containers.list(filters = {"name":"master1"})
		if len(existing_containers) > 0:
			logging.error("The container with name master1 already exists")
			exit(-1)
		logging.info("preparing master1")
		container = self.docker_client.containers.run(
			image=self.__image_name,
			detach=True,
			hostname="master1",
			name="master1",
			tty=True,
			mem_limit=self.config["memory_limit"],
			network=self.__network_name,
			ports={
				8080:8080,
				# 50070:50070,
			},
			environment={
				"BI2I_CLUSTER_TYPE":"MASTER"
			},
			volumes={ self.config["work_dir"]+"/assets": {
						'bind': '/opt/assets', 
						'mode': 'rw'
				},
			},
			# command="service ssh start &&   cp $SPARK_HOME/conf/slaves.template $SPARK_HOME/config/slaves &&  cat /opt/assets/workers.txt > $SPARK_HOME/conf/slaves && $SPARK_HOME/sbin/start-all.sh"
		)
		logging.info("prepared master1")

		logging.info("copying slaves file as backup ")
		container.exec_run("mv /opt/installed/spark-3.2.1-bin-hadoop3.2/conf/slaves.template /opt/installed/spark-3.2.1-bin-hadoop3.2/conf/slaves")


		logging.info("starting ssh service in master1")
		container.exec_run("service ssh start")

		
		logging.info("copying slaves file from asset to trigger spark on cluster ")
		container.exec_run("cp /opt/assets/workers.txt  /opt/installed/spark-3.2.1-bin-hadoop3.2/conf/slaves")
		
		self.__all_containers["master1"] = container
		

		logging.info("preparing list of workers")
		for worker_id in range(0,int(self.config["number_of_workers"])):
			ui_port = 8081+worker_id+1
			worker_name = "worker{}".format(worker_id+1)
			logging.info("preparing {}".format(worker_name))
			worker_container = self.docker_client.containers.run(
				image=self.__image_name,
				detach=True,
				hostname=worker_name,
				name=worker_name,
				tty=True,
				mem_limit=self.config["memory_limit"],
				network=self.__network_name,
				ports={
					8081:ui_port,
					# 50070:50070,
				},
				environment={
					"BI2I_CLUSTER_TYPE":"WORKER"
				},
				volumes={ self.config["work_dir"]+"/assets": {
							'bind': '/opt/assets', 
							'mode': 'rw'
					},
				},
				# command= ""
			)
			logging.info("starting ssh servie in {} ".format(worker_name))
			
			worker_container.exec_run("service ssh start")
			worker_container.exec_run("echo 'spark.ui.port %f' >> /opt/installed/spark-3.2.1-bin-hadoop3.2/conf/spark-defaults.conf".format(ui_port))
			logging.info("prepared {}".format(worker_name))
			self.__all_containers[worker_name] = worker_container

		
		logging.info("Running spark in cluster mode ")
		container.exec_run("/opt/installed/spark-3.2.1-bin-hadoop3.2/sbin/start-all.sh")
		# logging(self.__all_containers)
		print(self.__all_containers)

		try:
			while True:
				pass
		except KeyboardInterrupt as _ke:
			print("Got keyboard interrupt. Preparing to die")
			print("Please wait...")
			self.prepare_to_die()
			print("---complete---")

	def prepare_to_die(self):
		for item in self.__all_containers:
			logging.info("Attempting to remove container: {}".format(item))
			self.__all_containers[item].stop()
			self.__all_containers[item].remove()
			logging.info("Removed container: {}".format(item))
		networks = self.docker_client.networks.list(names = self.__network_name)
		logging.info("Attempting to remove network: {}".format(self.__network_name))
		for network in networks:
			short_id = network.short_id
			network.remove()
			logging.info("Removed network:{} with id:{}".format(self.__network_name, short_id))


def run(config_file):
	createCluster  = CreateCluster(config_file)
	createCluster.prepare_assets()
	createCluster.prepare_scripts()
	createCluster.prepare_network()
	createCluster.prepare_container()


if __name__ == "__main__":
	try:
		config_file = sys.argv[1]
		run(config_file)
	except	IndexOutOfBoundError as ie:
		print("Please provide config file as parameter")