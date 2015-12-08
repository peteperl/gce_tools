# gce_tools

Toolkit to setup a cluster on Google Compute Engine

## To Run

Make sure 'gcloud compute' is installed and authenticated  
 https://cloud.google.com/compute/docs/gcloud-compute/

Setup a preconfigured cluster: (look in gce_cluster.py)  

    gce_cluster.py <project> <cluster-name>

gce_cluster.py <project> <cluster-name>

Setup cluster with # and type of slaves:  
gce_cluster.py <project> <cluster-name> <no-slaves> <slave-type>

Kill the cluster:  
gce_cluster.py <project> <cluster-name> destroy
