#!/usr/bin/env python
#
# This script sets up a cluster on Google Compute Engine
#
# Make sure 'gcloud compute' is installed and authenticated 
# https://cloud.google.com/compute/docs/gcloud-compute/
###

__author__ = 'Pete C. Perlegos'

from __future__ import with_statement

import logging
import os
import pipes
import random
import shutil
import subprocess
import sys
import tempfile
import time
import commands
import urllib2
from optparse import OptionParser
from sys import stderr
import shlex
import getpass
import threading
import json

###
# Usage full : gce_cluster.py <project> <cluster-name> <no-slaves> <slave-type> <master-type> <identity-file> <zone>
# Usage short: gce_cluster.py <project> <cluster-name> <no-slaves> <slave-type>
# Usage test: gce_cluster.py <project> <cluster-name>
# Usage kill : gce_cluster.py <project> <cluster-name> destroy
###

username = ""
project = ""
cluster_name = ""
identity_file = ""
os_image = ""
slave_no = ""
slave_type = ""
master_type = ""
zone = ""

# regions
#regions = [us-east1-b, us-east1-c, us-east1-d, us-central1-a, us-central1-b, us-central1-c, us-central1-f]

# instance types
itypes = []
""" Automate this to always have updated image:  gcloud compute images list --uri
https://www.googleapis.com/compute/v1/projects/centos-cloud/global/images/centos-6-v20151104
https://www.googleapis.com/compute/v1/projects/centos-cloud/global/images/centos-7-v20151104
https://www.googleapis.com/compute/v1/projects/coreos-cloud/global/images/coreos-alpha-870-3-0-v20151124
https://www.googleapis.com/compute/v1/projects/coreos-cloud/global/images/coreos-beta-835-7-0-v20151120
https://www.googleapis.com/compute/v1/projects/coreos-cloud/global/images/coreos-stable-766-5-0-v20151105
https://www.googleapis.com/compute/v1/projects/debian-cloud/global/images/backports-debian-7-wheezy-v20151104
https://www.googleapis.com/compute/v1/projects/debian-cloud/global/images/debian-7-wheezy-v20151104
https://www.googleapis.com/compute/v1/projects/debian-cloud/global/images/debian-8-jessie-v20151104
https://www.googleapis.com/compute/v1/projects/google-containers/global/images/container-vm-v20151103
https://www.googleapis.com/compute/v1/projects/opensuse-cloud/global/images/opensuse-13-1-v20150822
https://www.googleapis.com/compute/v1/projects/opensuse-cloud/global/images/opensuse-13-2-v20150511
https://www.googleapis.com/compute/v1/projects/opensuse-cloud/global/images/opensuse-leap-42-1-v20151124
https://www.googleapis.com/compute/v1/projects/rhel-cloud/global/images/rhel-6-v20151104
https://www.googleapis.com/compute/v1/projects/rhel-cloud/global/images/rhel-7-v20151104
https://www.googleapis.com/compute/v1/projects/suse-cloud/global/images/sles-11-sp4-v20150714
https://www.googleapis.com/compute/v1/projects/suse-cloud/global/images/sles-12-v20150511
https://www.googleapis.com/compute/v1/projects/ubuntu-os-cloud/global/images/ubuntu-1204-precise-v20151119
https://www.googleapis.com/compute/v1/projects/ubuntu-os-cloud/global/images/ubuntu-1404-trusty-v20151113
https://www.googleapis.com/compute/v1/projects/ubuntu-os-cloud/global/images/ubuntu-1504-vivid-v20151120
https://www.googleapis.com/compute/v1/projects/ubuntu-os-cloud/global/images/ubuntu-1510-wily-v20151114
https://www.googleapis.com/compute/v1/projects/windows-cloud/global/images/windows-server-2008-r2-dc-v20151006
https://www.googleapis.com/compute/v1/projects/windows-cloud/global/images/windows-server-2012-r2-dc-v20151006
"""
image_centos_6 = 'https://www.googleapis.com/compute/v1/projects/centos-cloud/global/images/centos-6-v20141218'
image_ubuntu_14_04 = "https://www.googleapis.com/compute/v1/projects/ubuntu-os-cloud/global/images/ubuntu-1404-trusty-v20151113"

# default values
default_username = ""
default_identity_file = "gcompute"
default_slave_no = 2
default_slave_type = "n1-standard-1"
default_master_type = "n1-standard-1"
default_zone = "us-central1-a"

type_cpu = "n1-highcpu-32"
s_zone = "us-central1-f"
s_identity_file = "sp"


def read_args():

	global identity_file
	global slave_no
	global slave_type
	global master_type
	global zone
	global cluster_name
	global os_image
	global username
	global project

	if len(sys.argv) == 8:
		project = sys.argv[1]
		cluster_name = sys.argv[2]
		slave_no = int(sys.argv[3])
		slave_type = sys.argv[4]
		master_type = sys.argv[5]
		identity_file = sys.argv[6]
		zone = sys.argv[7]
		os_image = image_ubuntu_14_04
		username = getpass.getuser()

	elif len(sys.argv) == 3:
                print '*** Short ***'
		project = sys.argv[1]
		cluster_name = sys.argv[2]
		slave_no = default_slave_no
		slave_type = default_slave_type
		master_type = default_master_type
		username = default_username
		identity_file = default_identity_file
		zone = default_zone
		os_image = image_ubuntu_14_04
		print 'os_image:'
		print os_image
		username = getpass.getuser()

	elif len(sys.argv) == 4 and sys.argv[3].lower() == "test":
                print '*** Test ***'
		project = sys.argv[1]
		cluster_name = sys.argv[2]
                test()
                sys.exit(0)

	elif len(sys.argv) == 4 and sys.argv[3].lower() == "java":
                print '*** Install Java ***'
		project = sys.argv[1]
		cluster_name = sys.argv[2]
		username = default_username
		identity_file = default_identity_file
		(master_nodes, slave_nodes) = get_cluster_ips()
		install_java(master_nodes,slave_nodes)
                sys.exit(0)

	elif len(sys.argv) == 5 and sys.argv[3].lower() == "simple":
                print '*** Launching Instances ***'
		project = sys.argv[1]
		cluster_name = sys.argv[2]
		slave_no = int(sys.argv[4])
		username = default_username
		identity_file = s_identity_file
		slave_nodes = get_cluster_ips_simple()
                sys.exit(0)

	elif len(sys.argv) == 4 and sys.argv[3].lower() == "destroy":
		project = sys.argv[1]
		cluster_name = sys.argv[2]
		print '*** Destroying cluster ' + project + ' ***'
		try:
			command = 'gcloud compute --project ' + project + ' instances list --format json'
			output = subprocess.check_output(command, shell=True)
			data = json.loads(output)
			master_nodes=[]
			slave_nodes=[]
			kill_nodes=[]

			for instance in data:
				try:
					host_name = instance['name']	
					host_ip = instance['networkInterfaces'][0]['accessConfigs'][0]['natIP']

					if host_name == cluster_name + '-master':
						kill_nodes.append(host_name)
					elif cluster_name + '-slave' in host_name:
						kill_nodes.append(host_name)
				except:
					pass
                        destroy_hosts(project, kill_nodes)
                        destroy_network(project, cluster_name)
		except:
			print "Failed to Delete instances"
			sys.exit(1)
		sys.exit(0)

	else:
		print '# Usage full: gce_cluster.py <project> <cluster-name> <no-slaves> <slave-type> <master-type> <identity-file> <zone>'
		print '# Usage test: gce_cluster.py <project> <cluster-name>  (creates 2 small slaves)'
		print '# Usage kill: gce_cluster.py <project> <cluster-name> destroy'
		sys.exit(0)

def destroy_hosts(project, kill_list):

	print '[ Killing Nodes ]'
	
	try:
                kill_str = ''
                for killnode in kill_list:
                        kill_str = kill_str + killnode + ' '
                command = 'gcloud compute instances delete ' + kill_str + '--project ' + project
                command = shlex.split(command)
                subprocess.call(command)
                print '[ Killed: ' + kill_str + ']'
		
	except OSError:
		print "Failed to Delete instances. Exiting.."
		sys.exit(1)

def destroy_network(project, cluster_name):

	print '[ Killing Network]'	

	try:
                # Must delete Firewall before Network
		command = 'gcloud compute firewall-rules delete ' + cluster_name + '-internal' + ' --project ' + project
		command = shlex.split(command)
		subprocess.call(command)

		command = 'gcloud compute networks delete ' + cluster_name + '-network' + ' --project ' + project
		command = shlex.split(command)
		subprocess.call(command)

	except OSError:
		print "Failed to Delete Network. Exiting.."
		sys.exit(1)


def setup_network():

	print '[ Setting up Network & Firewall Entries ]'	

	try:
		command = 'gcloud compute --project=' + project + ' networks create ' + cluster_name + '-network --range 10.240.0.0/16'
		print command
		command = shlex.split(command)
		subprocess.call(command)

		command = 'gcloud compute firewall-rules create ' + cluster_name +'-internal --network ' + cluster_name + '-network --allow tcp udp icmp'
		print command
		command = shlex.split(command)
		subprocess.call(command)
		
	except OSError:
		print "Failed to setup Network & Firewall. Exiting.."
		sys.exit(1)

			
def launch_master():

	print '[ Launching Master ]'
	command = 'gcloud compute --project "' + project + '" instances create "' + cluster_name + '-master" --zone "' + zone + '" --machine-type "' + master_type + '" --network "' + cluster_name + '-network" --maintenance-policy "MIGRATE" --scopes "https://www.googleapis.com/auth/devstorage.read_only" --image "' + os_image + '" --boot-disk-type "pd-standard" --boot-disk-device-name "' + cluster_name + '-md"'
	print command
	command = shlex.split(command)
	subprocess.call(command)

def launch_slaves():
	
	print '[ Launching Slaves ]'
	### Need to parallelize slave setup
	for s_id in range(1,slave_no+1):
		command = 'gcloud compute --project "' + project + '" instances create "' + cluster_name + '-slave' + str(s_id) + '" --zone "' + zone + '" --machine-type "' + slave_type + '" --network "' + cluster_name + '-network" --maintenance-policy "MIGRATE" --scopes "https://www.googleapis.com/auth/devstorage.read_only" --image "' + os_image + '" --boot-disk-type "pd-standard" --boot-disk-device-name "' + cluster_name + '-s' + str(s_id) + 'd"'
		print command
		command = shlex.split(command)
		subprocess.call(command)

def launch_cluster():
	
	print '[ Creating the Cluster ]'
	setup_network()	
	launch_master()
	launch_slaves()

def launch_cluster_simple():
	
	print '[ Launching Cluster ]'
	### Need to parallelize slave setup
	for s_id in range(1,slave_no+1):
		command = 'gcloud compute --project "' + project + '" instances create "' + cluster_name + '-slave' + str(s_id) + '" --zone "' + s_zone + '" --machine-type "' + type_cpu + '" --network "default" --maintenance-policy "MIGRATE" --scopes "https://www.googleapis.com/auth/devstorage.full_control" --image "' + os_image + '" --boot-disk-type "pd-standard" --boot-disk-device-name "' + cluster_name + '-s' + str(s_id) + 'd"'
		print command
		command = shlex.split(command)
		#subprocess.call(command)

def check_gcloud():
	
	myexec = "gcloud"
	print '[ Verifying gcloud ]'
	try:
		subprocess.call([myexec, 'info'])
		
	except OSError:
		print "%s executable not found. \n# Make sure gcloud is installed and authenticated\nPlease follow https://cloud.google.com/compute/docs/gcloud-compute/" % myexec
		sys.exit(1)

def get_cluster_ips():
		
	command = 'gcloud compute --project ' + project + ' instances list --format json'
	output = subprocess.check_output(command, shell=True)
	data = json.loads(output)
	master_nodes=[]
	slave_nodes=[]

	for instance in data:
		try:
			host_name = instance['name']
			host_ip = instance['networkInterfaces'][0]['accessConfigs'][0]['natIP']
			if host_name == cluster_name + '-master':
				master_nodes.append(host_ip)
			elif cluster_name + '-slave' in host_name:
				slave_nodes.append(host_ip)
		except:
			pass
	
	# Return all the instances
	return (master_nodes, slave_nodes)

def get_cluster_ips_simple():
		
	command = 'gcloud compute --project ' + project + ' instances list --format json'
	output = subprocess.check_output(command, shell=True)
	data = json.loads(output)
	slave_nodes=[]

	for instance in data:
		try:
			host_ip = instance['networkInterfaces'][0]['accessConfigs'][0]['natIP']
			slave_nodes.append(host_ip)
		except:
			pass
	
	# Return all the instances
	return slave_nodes

def enable_sudo(host,command):
        command = "ssh -i " + identity_file + " -t -o 'UserKnownHostsFile=/dev/null' -o 'CheckHostIP=no' -o 'StrictHostKeyChecking no' "+ username + "@" + host + " '" + command + "'"
        print command
	os.system(command)

def ssh_thread(host,command):

	enable_sudo(host,command)

def update_nodes(master_nodes,slave_nodes):
        # Ubuntu
	print '[ Updataing Nodes ]'
        command = "sudo apt-get -y update"

	if master_nodes:
                master = master_nodes[0]
                master_thread = threading.Thread(target=ssh_thread, args=(master, command))
                master_thread.start()
	
	for slave in slave_nodes:
		slave_thread = threading.Thread(target=ssh_thread, args=(slave, command))
		slave_thread.start()
	
	slave_thread.join()
	master_thread.join()
	os.system("reset")
	
def install_java_rhel(master_nodes,slave_nodes):
        # RHEL/CentOS
	print '[ Installing Java and Development Tools ]'
	command = ""

	if master_nodes:
                master = master_nodes[0]
                master_thread = threading.Thread(target=ssh_thread, args=(master,"sudo yum install -y java-1.7.0-openjdk;sudo yum install -y java-1.7.0-openjdk-devel;sudo yum groupinstall \'Development Tools\' -y"))
                master_thread.start()
	
	for slave in slave_nodes:
		slave_thread = threading.Thread(target=ssh_thread, args=(slave,"sudo yum install -y java-1.7.0-openjdk;sudo yum install -y java-1.7.0-openjdk-devel;sudo yum groupinstall \'Development Tools\' -y"))
		slave_thread.start()
	
	slave_thread.join()
	master_thread.join()
	os.system("reset")

def install_java(master_nodes,slave_nodes):
        # Ubuntu
	print '[ Installing Java ]'
	command = ""

	if master_nodes:
                master = master_nodes[0]
                master_thread = threading.Thread(target=ssh_thread, args=(master,"sudo apt-get -y install default-jre;sudo apt-get -y install default-jdk"))
                master_thread.start()
	
	for slave in slave_nodes:
		slave_thread = threading.Thread(target=ssh_thread, args=(slave,"sudo apt-get -y install default-jre;sudo apt-get -y install default-jdk"))
		#slave_thread = threading.Thread(target=ssh_thread, args=(slave,"sudo apt-get -y install openjdk-7-jre;sudo apt-get -y install openjdk-7-jdk"))
		slave_thread.start()
	
	slave_thread.join()
	master_thread.join()
	os.system("reset")

def ssh_command(host,command):
	
	#print "ssh -i " + identity_file + " -o 'UserKnownHostsFile=/dev/null' -o 'CheckHostIP=no' -o 'StrictHostKeyChecking no' "+ username + "@" + host + " '" + command + "'"
	commands.getstatusoutput("ssh -i " + identity_file + " -o 'UserKnownHostsFile=/dev/null' -o 'CheckHostIP=no' -o 'StrictHostKeyChecking no' "+ username + "@" + host + " '" + command + "'" )
	
	
def deploy_keys(master_nodes,slave_nodes):

	print '[ Generating SSH Keys on Master ]'
	# For now just add your public keys to the Metadata on your project


def attach_drive(master_nodes,slave_nodes):

	print '[ Adding new 500GB drive on Master ]'
	master = master_nodes[0]
	### COMING SOON


def test():

        print '*** Test ***'

        #Get Master/Slave IP Addresses
	(master_nodes, slave_nodes) = get_cluster_ips()
	#print master_nodes
	#print slave_nodes

	#Install Java and build-essential
	#install_java(master_nodes,slave_nodes)

	#Generate SSH keys and deploy
	#deploy_keys(master_nodes,slave_nodes)

	#Attach a new empty drive and format it
	#attach_drive(master_nodes,slave_nodes)

        print '*** Test Done ***'

def real_main():

	print "[ Setup Script Started ]"	
	#Read the arguments
	read_args()
	#Make sure gcloud is accessible.
	check_gcloud()

	#Launch the cluster
	launch_cluster()

	#Wait some time for machines to bootup
	waitTime = 120   # recommend 120 seconds
	print '[ Waiting ' + str(waitTime) + ' Seconds for Machines to start up ]'
	time.sleep(waitTime)

	#Get Master/Slave IP Addresses
	(master_nodes, slave_nodes) = get_cluster_ips()

	#Install Java and build-essential
	install_java(master_nodes,slave_nodes)

	#Generate SSH keys and deploy
	#deploy_keys(master_nodes,slave_nodes)

	#Attach a new empty drive and format it
	#attach_drive(master_nodes,slave_nodes)

	#Set up Spark/Shark/Hadoop
	#setup_spark(master_nodes,slave_nodes)

def launch_main():

	print "[ Launch Script Started ]"	
	#Read the arguments
	read_args()
	#Make sure gcloud is accessible.
	check_gcloud()
	
	#Launch the cluster
	launch_cluster_simple()

	#Wait some time for machines to bootup
	waitTime = 90   # recommend 120 seconds
	print '[ Waiting ' + str(waitTime) + ' Seconds for Machines to start up ]'
	time.sleep(waitTime)

	#Get Master/Slave IP Addresses
	(master_nodes, slave_nodes) = get_cluster_ips()

        #Update Nodes
	update_nodes(master_nodes,slave_nodes)
	print '[ Updated Nodes: Waiting ' + str(waitTime) + ' Seconds ]'
	time.sleep(waitTime)

	#Install Java
	install_java(master_nodes,slave_nodes)
	pprint '[ Installing Java: Waiting ' + str(waitTime) + ' Seconds ]'
	time.sleep(waitTime)

def launch_simple():

	print "[ Launch Script Started ]"	
	#Read the arguments
	read_args()
	#Make sure gcloud is accessible.
	check_gcloud()
	
	#Launch the cluster
	launch_cluster()

	#Wait some time for machines to bootup
	waitTime = 90   # recommend 120 seconds
	print '[ Waiting ' + str(waitTime) + ' Seconds for Machines to start up ]'
	time.sleep(waitTime)

	#Get Master/Slave IP Addresses
	slave_nodes = get_cluster_ips_simple()
	print slave_nodes
	"""

        #Update Nodes
	update_nodes(None,slave_nodes)
	print '[ Updated Nodes: Waiting ' + str(waitTime) + ' Seconds ]'
	time.sleep(waitTime)

	#Install Java
	install_java(None,slave_nodes)
	print '[ Installing Java: Waiting ' + str(waitTime) + ' Seconds ]'
	time.sleep(waitTime)
	"""


def main():
  try:
    #real_main()
    #launch_main()
    launch_simple()
  except Exception as e:
    print >> stderr, "\nError:\n", e
    

if __name__ == "__main__":
  main()
