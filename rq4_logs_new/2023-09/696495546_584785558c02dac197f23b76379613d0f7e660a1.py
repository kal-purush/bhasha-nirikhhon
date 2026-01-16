"""A Python Pulumi program"""

import pulumi
from pulumi_digitalocean import kubernetes

# Define the DO token.
do_token = pulumi.Config("do_token")

# Create a new Digital Ocean Kubernetes cluster in the sfo2 region.
do_cluster = kubernetes.Cluster("doCluster",
    region="sfo2",
    version="1.20.2-do.0",
    node_pool={
        "name": "worker-pool",
        "size": "s-2vcpu-2gb",
        "nodeCount": 3
    },
    opts=pulumi.ResourceOptions(
        environs={ "DIGITALOCEAN_TOKEN": do_token }
    )
)

# Export the cluster's endpoint and Kubeconfig.
pulumi.export("clusterEndpoint", do_cluster.endpoint)
pulumi.export("clusterKubeconfig", do_cluster.kube_configs[0]["rawConfig"])