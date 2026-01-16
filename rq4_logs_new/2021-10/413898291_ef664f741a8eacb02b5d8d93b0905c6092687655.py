#!/usr/bin/python3

import datetime
import os
import time
import sys
import argparse
import pytz
import logging

from kubernetes import client, config
from kubernetes.client.rest import ApiException
from tqdm import tqdm

LOGLEVEL=logging.INFO

def createDeploymentConfig(replicasCount, nginxVersion, deploymentName):
    # Create Deployment Configuration
    container = client.V1Container(
        name="nginx",
        image="nginx:" +
        nginxVersion + "",
        ports=[client.V1ContainerPort(container_port=9090)],
        security_context=client.V1Capabilities(
            drop=["ALL"], add=["NET_BIND_SERVICE", "NET_ADMIN", "SYS_TIME"]),
        resources=client.V1ResourceRequirements(
            requests={
                "cpu": "100m",
                "memory": "200Mi"
            },
            limits={
                "cpu": "500m",
                "memory": "500Mi"
            },
        ),
    )
    template = client.V1PodTemplateSpec(
        metadata=client.V1ObjectMeta(labels={"app": "nginx"}),
        spec=client.V1PodSpec(containers=[container]))
    spec = client.V1DeploymentSpec(replicas=replicasCount,
                                   template=template,
                                   selector={"matchLabels": {
                                       "app": "nginx"
                                   }})
    deploymentConfig = client.V1Deployment(
        api_version="apps/v1",
        kind="Deployment",
        metadata=client.V1ObjectMeta(name=deploymentName),
        spec=spec,
    )
    return deploymentConfig

def createServiceConfig(deploymentName):
    spec = client.V1ServiceSpec(
        ports=[client.V1ServicePort(port=9090, protocol="TCP")],
        selector={"app": "nginx"})
    serviceConfig = client.V1Service(
        api_version="v1",
        kind="Service",
        metadata=client.V1ObjectMeta(name=deploymentName,
                                     namespace="default",
                                     labels={"app": "nginx"}),
        spec=spec,
    )
    return serviceConfig

def progressBar(counts, DESCRIPTION):
    for i in tqdm(range(counts), desc=DESCRIPTION):
        time.sleep(0.5)

def getDeploymentObject(appsV1API, deploymentConfig, coreV1API, serviceConfig, replicasCount, nginxVersion,
                   deploymentName):
    deploymentStatusCode = 200
    serviceStatusCode = 200
    try:
        deploymentResponse = appsV1API.read_namespaced_deployment(name=deploymentName,
                                              namespace="default")
        old_Image = deploymentResponse.spec.template.spec.containers[0].image
        old_nginxVersion = old_Image.split(":", 1)[1]
        old_replicasCount = deploymentResponse.spec.replicas
        print("\nOld Replica: " + str(old_replicasCount) + "\nNew Replica: " +
              str(replicasCount) + "\n\nOld Image Version: " +
              str(old_nginxVersion) + "\nNew Image Version: " +
              str(nginxVersion) + "\n")
    except ApiException as e:
        deploymentStatusCode = e.status

    try:
        serviceResponse = coreV1API.read_namespaced_service(name=deploymentName,
                                                   namespace="default")
    except ApiException as e:
        serviceStatusCode = e.status

    if (deploymentStatusCode == 404 and serviceStatusCode == 404):
        print("\nCreating Deployment " + deploymentName + "\n")
        createDeploymentObject(appsV1API, deploymentConfig, deploymentName)
        createServiceObject(coreV1API, serviceConfig, deploymentName)
    else:
        if (old_replicasCount != replicasCount or old_nginxVersion != nginxVersion):
            print("\nUpdating deploymentConfig\n")
            deploymentConfig.spec.template.spec.containers[
                0].image = "nginx:" + str(
                    nginxVersion) + ""
            deploymentConfig.spec.replicas = replicasCount
            update_deployment(appsV1API, deploymentConfig, coreV1API, serviceConfig,
                              deploymentName)
        else:
            progressBar(replicasCount, "Info")
            print("\n\nNo change in DeploymentSpec")


def createDeploymentObject(appsV1API, deploymentConfig, deploymentName):
    # Create deploymentConfig
    deploymentResponse = appsV1API.create_namespaced_deployment(body=deploymentConfig, namespace="default")
    replicasCount = deploymentResponse.spec.replicas
    progressBar(replicasCount, "createDeploymentObject ")
    print("\n\n[INFO] Deployment " + deploymentName + " created.\n")
    print("%s\t%s\t\t\t%s\t%s" % ("NAMESPACE", "NAME", "REVISION", "IMAGE"))
    print("%s\t\t%s\t%s\t\t%s\n" % (
        deploymentResponse.metadata.namespace,
        deploymentResponse.metadata.name,
        deploymentResponse.metadata.generation,
        deploymentResponse.spec.template.spec.containers[0].image,
    ))


def createServiceObject(appsV1API, serviceConfig, deploymentName):
    # Create serviceConfig
    deploymentResponse = appsV1API.create_namespaced_service(body=serviceConfig, namespace="default")
    print("\n[INFO] Service " + deploymentName + " created.\n")
    print("%s\t%s" % ("NAMESPACE", "NAME"))
    print("%s\t\t%s\n" % (
        deploymentResponse.metadata.namespace,
        deploymentResponse.metadata.name,
    ))


def update_deployment(appsV1API, deploymentConfig, coreV1API, serviceConfig, deploymentName):
    # patch the deploymentConfig
    deploymentResponse = appsV1API.patch_namespaced_deployment(name=deploymentName,
                                           namespace="default",
                                           body=deploymentConfig)

    serviceResponse = coreV1API.patch_namespaced_service(name=deploymentName,
                                                namespace="default",
                                                body=serviceConfig)

    replicasCount = deploymentResponse.spec.replicas
    progressBar(replicasCount, "updateDeployment ")

    print("\n[INFO] Deployment " + deploymentName + " updated.\n")
    print("%s\t%s\t\t\t%s\t%s" % ("NAMESPACE", "NAME", "REVISION", "IMAGE"))
    print("%s\t\t%s\t%s\t\t%s\n" % (
        deploymentResponse.metadata.namespace,
        deploymentResponse.metadata.name,
        deploymentResponse.metadata.generation,
        deploymentResponse.spec.template.spec.containers[0].image,
    ))

    print("\n[INFO] Service " + deploymentName + " updated.\n")
    print("%s\t%s" % ("NAMESPACE", "NAME"))
    print("%s\t\t%s\n" % (
        serviceResponse.metadata.namespace,
        serviceResponse.metadata.name,
    ))


def deleteDeploymentObject(appsV1API, coreV1API, replicasCount, deploymentName):
    # Delete deployment and service
    deploymentStatusCode = 200
    serviceStatusCode = 200

    # Check existing deployments and services if any and Delete
    try:
        deploymentResponse = appsV1API.read_namespaced_deployment(name=deploymentName,
                                              namespace="default")
    except ApiException as e:
        deploymentStatusCode = e.status

    try:
        serviceResponse = coreV1API.read_namespaced_service(name=deploymentName,
                                                   namespace="default")
    except ApiException as e:
        serviceStatusCode = e.status

    if (deploymentStatusCode == 404 and serviceStatusCode == 404):
        print("\nNo Such Deployment " + deploymentName + "found\n")
    else:
        deploymentResponse = appsV1API.delete_namespaced_deployment(
            name=deploymentName,
            namespace="default",
            body=client.V1DeleteOptions(propagation_policy="Foreground",
                                        grace_period_seconds=2))
        serviceResponse = coreV1API.delete_namespaced_service(
            name=deploymentName,
            namespace="default",
        )
        print("\n")
        progressBar(replicasCount, "deleteDeployment ")
        print("\n\n[INFO] Deployment & Service " + deploymentName +
              " deleted.\n")


def main(argv):

    parser = argparse.ArgumentParser(
        prog='knd',
        description=
        'knd (Kubernetes NGINX deployer) deploys NGINX on a Kubernetes cluster, and verifies that it has come up healthy.'
    )
    parser.add_argument(
        '-r',
        '--replicasCount',
        type=int,
        required=False,
        help='Input the desired number of replicasCount for your application. Default is 1',
        default=1)
    parser.add_argument(
        '-nv',
        '--nginxVersion',
        metavar='--nginx-version',
        required=False,
        help='Enter the nginx version to deploy. Default is 1.20.1',
        type=str,
        default="1.20.1")
    parser.add_argument(
        '-d',
        '--deploymentName',
        metavar='--deployment-name',
        required=True,
        help='Enter the deployment name. Example: nginx-deployment')
    parser.add_argument(
        '-D',
        '--deleteDeployment',
        metavar='--delete-deployment',
        required=False,
        help='Enter yes or no. Default is no',
        type=str,
        default="no")

    args = parser.parse_args()

    config.load_kube_config()
    apps_v1 = client.AppsV1Api()
    coreV1API = client.CoreV1Api()

    replicasCount = args.replicasCount
    nginxVersion = args.nginxVersion
    deploymentName = args.deploymentName
    deleteDeployment = args.deleteDeployment

    deploymentConfig = createDeploymentConfig(replicasCount, nginxVersion,
                                          deploymentName)
    serviceConfig = createServiceConfig(deploymentName)

    if deleteDeployment == "yes":
        deleteDeploymentObject(apps_v1, coreV1API, replicasCount, deploymentName)
    else:
        getDeploymentObject(apps_v1, deploymentConfig, coreV1API, serviceConfig, replicasCount,
                       nginxVersion, deploymentName)

if __name__ == "__main__":
    main(sys.argv)