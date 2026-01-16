from tempfile import mkdtemp
import shutil
import os
import xml.etree.ElementTree as ET
import zipfile

# changeables: App Name, Deployment Path Name, Cloud Provider Name


def zippit(source_path, zip_name, files, destination_path):
    with zipfile.ZipFile(os.path.join(destination_path, zip_name), mode='w') as zf:
        for file in files:
            zf.write(os.path.join(source_path, file), arcname=file)


def get_files(path):
    files = []
    for (dirpath, dirnames, filenames) in os.walk(path):
        for filename in filenames:
            relDir = os.path.relpath(dirpath, path)
            relFile = os.path.join(relDir, filename)
            files.append(relFile)
    return files


class AwsParameters:
    def __init__(self, ami_id, instance_type, region_name):
        self._instance_type = instance_type
        self._ami_id = ami_id
        self._region_name = region_name

    @property
    def instance_type(self):
        return self._instance_type

    @property
    def ami_id(self):
        return self._ami_id

    @property
    def region_name(self):
        return self._region_name


class AzureParameters:
    def __init__(self, region_name, image_publisher, image_offer, image_sku, image_version, vm_size):
        self._region_name = region_name
        self._image_publisher = image_publisher
        self._image_offer = image_offer
        self._image_sku = image_sku
        self._image_version = image_version
        self._vm_size = vm_size

    @property
    def vm_size(self):
        return self._vm_size

    @property
    def region_name(self):
        return self._region_name

    @property
    def image_publisher(self):
        return self._image_publisher

    @property
    def image_offer(self):
        return self._image_offer

    @property
    def image_sku(self):
        return self._image_sku

    @property
    def image_version(self):
        return self._image_version


class CloudshellPackage:
    def __init__(self, src_package_path):
        self.src = src_package_path

    def prepare_aws_package(self, aws_parameters, app_name, dst_path, cloud_provider_name='AWS EC2'):
        cp_modifications = {'ami_id': aws_parameters.ami_id,
                            'Instance Type': aws_parameters.instance_type}
        short_provider_name = 'aws'
        self._prepare_package(cp_modifications, app_name, cloud_provider_name, dst_path, aws_parameters.region_name,
                              short_provider_name, CloudshellPackage._aws_specific_modifications)

    def prepare_azure_package(self, azure_parameters, app_name, dst_path, cloud_provider_name='Azure'):
        short_provider_name = 'azure'
        self._prepare_package(azure_parameters, app_name, cloud_provider_name, dst_path, azure_parameters.region_name,
                              short_provider_name, CloudshellPackage._azure_specific_modifications)

    def _prepare_package(self, cp_modifications, app_name, cloud_provider_name, dst_path, region_name,
                         short_provider_name, cp_modifier_func):
        source_dir = os.path.join(mkdtemp(), short_provider_name)
        try:
            shutil.copytree(self.src, source_dir)
            datamodel_path = self._get_app_datamodel_file(source_dir)
            self._modify_datamodel_file(cp_modifications, app_name, cloud_provider_name, datamodel_path, region_name,
                                        cp_modifier_func)
            self._prepare_package_archive(app_name, source_dir, dst_path, region_name, short_provider_name)
        except Exception as e:
            shutil.rmtree(source_dir)
            raise e

    def _prepare_package_archive(self, app_name, package_source_path, dst_path, region_name, short_provider_name):
        archive_name = self._get_package_archive_name(app_name, region_name, short_provider_name)
        package_files = get_files(package_source_path)
        zippit(package_source_path, archive_name, package_files, dst_path)

    @staticmethod
    def _get_package_archive_name(app_name, region_name, short_provider_name):
        return '{0}_{1}_{2}.zip'.format(app_name, short_provider_name, region_name).replace(' ', '_')

    @staticmethod
    def _modify_datamodel_file(cp_values, app_name, cloud_provider_name, datamodel_path, region_name,
                               cp_modifier_func):
        tree = ET.parse(datamodel_path)
        app_root = tree.getroot()
        app_resource_info = app_root.find('./AppResourceInfo')
        app_resource_info.attrib['Name'] = app_name
        deployment_path = app_root.findall('.//DeploymentPath')[0]
        deployment_path_name = '{0} {1}'.format(cloud_provider_name, region_name)
        deployment_path.attrib['Name'] = deployment_path_name
        cp_modifier_func(cp_values, deployment_path)
        with open(datamodel_path, 'w') as f:
            tree.write(f)

    @staticmethod
    def _aws_specific_modifications(cp_modifications, deployment_path):
        ami_id_element = deployment_path.findall("./DeploymentService//Attribute[@Name='AWS AMI Id']")[0]
        ami_id_element.attrib['Value'] = cp_modifications['ami_id']
        ami_id_element = deployment_path.findall("./DeploymentService//Attribute[@Name='Instance Type']")[0]
        ami_id_element.attrib['Value'] = cp_modifications['Instance Type']

    @staticmethod
    def _azure_specific_modifications(azure_params, deployment_path):
        """
        :type azure_params: AzureParameters
        :type deployment_path: xml.etree.Element
        :return:
        """
        cp = CloudshellPackage
        cp._modify_deployment_path_attribute(deployment_path, 'Image Publisher', azure_params.image_publisher)
        cp._modify_deployment_path_attribute(deployment_path, 'Image Offer', azure_params.image_offer)
        cp._modify_deployment_path_attribute(deployment_path, 'Image SKU', azure_params.image_sku)
        cp._modify_deployment_path_attribute(deployment_path, 'Image Version', azure_params.image_version)
        cp._modify_deployment_path_attribute(deployment_path, 'VM Size', azure_params.vm_size)

    @staticmethod
    def _modify_deployment_path_attribute(deployment_path, attribute, value):
        element = deployment_path.findall("./DeploymentService//Attribute[@Name='{0}']".format(attribute))[0]
        element.attrib['Value'] = value
        return deployment_path

    @staticmethod
    def _get_app_datamodel_file(aws_dir):
        app_xml_path = os.path.join(aws_dir, 'App Templates', 'app.xml')
        return app_xml_path




