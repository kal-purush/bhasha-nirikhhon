from googleapiclient import discovery
import os
import time


class GcloudInstance:

    def __init__(self, service_name: str, version: str, credentials_path: str):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
        self.compute = discovery.build(service_name, version)  # Google API resource object
        self.operation = None
        self.project = None
        self.zone = None
        self.name = None
        self.bucket = None

    def get_existing_instance(self, project='restorationbot', zone='us-west1-b', name='pytorch-1-4-gpu-vm'):
        self.project = project
        self.zone = zone
        self.name = name
        self.operation = self.compute.instances().get(
            project=self.project,
            zone=self.zone,
            instance=self.name).execute()

    def status(self):
        if self.operation is None:
            return "Object has no current operation/instance."
        return self.operation['status']

    def _wait_for_operation(self) -> object:

        while self.operation is not None:
            result = self.compute.zoneOperations().get(
                project=self.project,
                zone=self.zone,
                operation=self.operation).execute()

            if result['status'] == 'DONE':
                if 'error' in result:
                    raise Exception(result['error'])
                return result

            time.sleep(1)

    def delete_instance(self) -> None:
        return self.compute.instances().delete(
            project=self.project,
            zone=self.zone,
            instance=self.name).execute()

    def stop_instance(self):
        return self.compute.instances().stop(
            project=self.project,
            zone=self.zone,
            instance=self.name).execute()

    def start_instance(self):
        if self.operation is not None:
            self.compute.instances().start(
                project=self.project,
                zone=self.zone,
                instance=self.name).execute()

    def create_instance(self, project: str, zone='us-west1-b', name='restorebot-instance',
                        bucket='reddit_object_bucket', startup_script='startup-script.sh',
                        machine_type='n1-standard-2') -> None:
        '''

        :param project: the name of the gcloud project to use
        :param zone: the zone name
        :param name: the name of the VM instance
        :param bucket: the name of the existing bucket to use
        :param startup_script: the startup script file name
        :param machine_type: the machine type to create. See https://cloud.google.com/compute/docs/machine-types
        :return: None
        '''
        self.project = project
        self.zone = zone
        self.name = name
        self.bucket = bucket

        # Get the latest Debian Jessie image.
        image_response = self.compute.images().getFromFamily(
            project='deeplearning-platform-release', family='pytorch-latest-gpu').execute()
        source_disk_image = image_response['selfLink']

        # Configure the machine
        machine_type = "zones/{}/machineTypes/{}".format(zone, machine_type)
        startup_script = open(
            os.path.join(
                os.path.dirname(__file__), startup_script), 'r').read()
        image_url = "http://storage.googleapis.com/gce-demo-input/photo.jpg"
        image_caption = "Ready for dessert?"

        config = self._create_config(startup_script, machine_type, source_disk_image)
        # config = self._create_config_(startup_script)

        self.operation = self.compute.instances().insert(
            project=project,
            zone=zone,
            body=config).execute()

    def _create_config_(self, startup_script):
        config = {
            "kind": "compute#instance",
            "name": "restorebot-instance",
            "zone": "projects/restorationbot/zones/us-west1-b",
            "machineType": "projects/restorationbot/zones/us-west1-b/machineTypes/n1-standard-2",
            "displayDevice": {
                "enableDisplay": False
            },
            "metadata": {
                "kind": "compute#metadata",
                "items": [
                    {
                        "key": "gce-container-declaration",
                        "value": "spec:\n  containers:\n    - name: restorebot-instance\n      image: gcr.io/deeplearning-platform-release/pytorch-gpu\n      stdin: false\n      tty: false\n  restartPolicy: Always\n\n# This container declaration format is not public API and may change without notice. Please\n# use gcloud command-line tool or Google Cloud Console to run Containers on Google Compute Engine."
                    },
                    {
                        "key": "google-logging-enabled",
                        "value": "true"
                    },
                    {
                        'key': 'startup-script',
                        'value': startup_script
                    }
                ]
            },
            "tags": {
                "items": []
            },
            "guestAccelerators": [
                {
                    "acceleratorCount": 1,
                    "acceleratorType": "projects/restorationbot/zones/us-west1-b/acceleratorTypes/nvidia-tesla-k80"
                }
            ],
            "disks": [
                {
                    "kind": "compute#attachedDisk",
                    "type": "PERSISTENT",
                    "boot": True,
                    "mode": "READ_WRITE",
                    "autoDelete": True,
                    "deviceName": "restorebot-instance",
                    "initializeParams": {
                        "sourceImage": "projects/cos-cloud/global/images/cos-stable-85-13310-1041-14",
                        "diskType": "projects/restorationbot/zones/us-west1-b/diskTypes/pd-standard",
                        "diskSizeGb": "70"
                    },
                    "diskEncryptionKey": {}
                }
            ],
            "canIpForward": False,
            "networkInterfaces": [
                {
                    "kind": "compute#networkInterface",
                    "subnetwork": "projects/restorationbot/regions/us-west1/subnetworks/default",
                    "accessConfigs": [
                        {
                            "kind": "compute#accessConfig",
                            "name": "External NAT",
                            "type": "ONE_TO_ONE_NAT",
                            "networkTier": "PREMIUM"
                        }
                    ],
                    "aliasIpRanges": []
                }
            ],
            "description": "",
            "labels": {
                "container-vm": "cos-stable-85-13310-1041-14"
            },
            "scheduling": {
                "preemptible": True,
                "onHostMaintenance": "TERMINATE",
                "automaticRestart": True,
                "nodeAffinities": []
            },
            "deletionProtection": False,
            "reservationAffinity": {
                "consumeReservationType": "ANY_RESERVATION"
            },
            "serviceAccounts": [
                {
                    "email": "520719769055-compute@developer.gserviceaccount.com",
                    "scopes": [
                        "https://www.googleapis.com/auth/devstorage.read_only",
                        "https://www.googleapis.com/auth/logging.write",
                        "https://www.googleapis.com/auth/monitoring.write",
                        "https://www.googleapis.com/auth/servicecontrol",
                        "https://www.googleapis.com/auth/service.management.readonly",
                        "https://www.googleapis.com/auth/trace.append"
                    ]
                }
            ],
            "shieldedInstanceConfig": {
                "enableSecureBoot": False,
                "enableVtpm": True,
                "enableIntegrityMonitoring": True
            },
            "confidentialInstanceConfig": {
                "enableConfidentialCompute": False
            }
        }
        return config

    def _create_config(self, startup_script, machine_type: str, source_disk_image: str) -> {}:
        config = {
            'name': self.name,
            'machineType': machine_type,

            # Specify the boot disk and the image to use as a source.
            'disks': [
                {
                    'boot': True,
                    'autoDelete': True,
                    'initializeParams': {
                        'sourceImage': source_disk_image,
                    }
                }
            ],

            # Specify a network interface with NAT to access the public
            # internet.
            'networkInterfaces': [{
                'network': 'global/networks/default',
                'accessConfigs': [
                    {'type': 'ONE_TO_ONE_NAT', 'name': 'External NAT'}
                ]
            }],

            # Allow the instance to access cloud storage and logging.
            'serviceAccounts': [{
                'email': 'default',
                'scopes': [
                    'https://www.googleapis.com/auth/devstorage.read_write',
                    'https://www.googleapis.com/auth/logging.write'
                ]
            }],

            # Metadata is readable from the instance and allows you to
            # pass configuration from deployment scripts to instances.
            'metadata': {
                'items': [{
                    # Startup script is automatically executed by the
                    # instance upon startup.
                    'key': 'startup-script',
                    'value': startup_script
                }, {
                    'key': 'bucket',
                    'value': self.bucket
                },
                    {
                        'install-nvidia-driver': True
                    }]
            },
            'guestAccelerators': [
                {
                    'acceleratorCount': 1,
                    'acceleratorType': "projects/{}/zones/{}/acceleratorTypes/nvidia-tesla-k80".format(self.project,
                                                                                                       self.zone)
                }
            ],
            'scheduling': {
                'preemptible': False,
                "onHostMaintenance": "TERMINATE",
                "automaticRestart": False
            },
            "deletionProtection": False
        }

        return config