#!/usr/bin/env python3
#----------------------------------------------------------------------------------
# Project Name      - supervisor install/clone_repos.py
# Started On        - Wed 26 Feb 14:43:08 CET 2020
# Last Change       - Wed 26 Feb 14:43:08 CET 2020
# Author E-Mail     - spuny4@gmail.com
# Author GitHub     - https://github.com/spuny
#----------------------------------------------------------------------------------
# This little script clones git repositories needed to install the lab env

from git import Repo

def main():
    git_urls = {
        "openstack" : "git@github.com:spuny/openstack-helm.git",
        "openstack_infra" : "git@github.com:spuny/openstack-helm-infra.git",
        "kubespray" : "git@github.com:kubernetes-sigs/kubespray.git",
        "armada" : "git@github.com:spuny/armada-manifests.git"
    }
    git_folders = {
            "openstack" : "./openstack-helm",
            "openstack_infra" : "./openstack-helm-infra",
            "kubespray" : "./kubespray",
            "armada" : "./armada-manifests"
    }
    for i in git_urls:
        print ("Clonning {} repository to {}".format(git_urls[i],
            git_folders[i]))
        Repo.clone_from(git_urls[i], git_folders[i])

if __name__ == '__main__':
    main()