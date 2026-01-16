from pandas import ExcelFile
import math

from src.regionify.cp_parameters import AwsParameters, AzureParameters


class AppInvoice:
    def __init__(self, path_to_excel_file):
        excel = ExcelFile(path_to_excel_file)
        self._invoice = excel.parse(excel.sheet_names[0]).to_dict('index')

    def aws_cloud_provider_params(self):
        AWS_NONE_REGION = ['OS', 'App Name', 'Instance Type']
        params = []
        for key, app in self._invoice.iteritems():
            regions = [region for region in app if region not in AWS_NONE_REGION]
            for region in regions:
                if is_not_nan(app[region]):
                    params.append(AwsParameters(ami_id=app[region],
                                                region_name=region,
                                                app_name=app['App Name'],
                                                instance_type=app['Instance Type']))
                continue
        return params

    def azure_cloud_provider_params(self):
        AZURE_NONE_REGION = ['App Name', 'OS', 'Sku', 'Image Publisher', 'Image Offer', 'VM Size']
        params = []
        for key, app in self._invoice.iteritems():
            regions = [region for region in app if
                       region not in AZURE_NONE_REGION]
            for region in regions:
                if is_not_nan(app[region]):
                    params.append(AzureParameters(region_name=region,
                                                  image_offer=app['Image Offer'],
                                                  image_publisher=app['Image Publisher'],
                                                  image_sku=str(app['Sku']),
                                                  app_name=app['App Name'],
                                                  vm_size=app['VM Size']))
                continue
        return params


def is_not_nan(value):
    try:
        math.isnan(value)
        return False
    except TypeError:
        return True