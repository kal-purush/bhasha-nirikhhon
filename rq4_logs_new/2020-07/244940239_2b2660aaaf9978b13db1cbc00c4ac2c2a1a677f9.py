from os.path import expanduser

from infraxys.json.json_utils import JsonUtils
from aliyunsdkcore.client import AcsClient
from infraxys.logger import Logger


class AliyunHelper(object):

    @staticmethod
    def get_client(profile_name):
        logger = Logger.get_logger(AliyunHelper.__class__.__name__)
        logger.info("Retrieving Aliyun profile details from ~/.aliyun/config.json")
        user_home = expanduser("~")

        config_json = JsonUtils.get_instance().load_from_file(f'{user_home}/.aliyun/config.json')

        for profile_json in config_json['profiles']:
            if profile_json['name'] == profile_name:
                access_key_id = profile_json['access_key_id']
                access_key_secret = profile_json['access_key_secret']
                region_id = profile_json['region_id']

        if not access_key_id:
            raise Exception(f'Aliyun profile {profile_name} not found.')

        client = AcsClient(ak=access_key_id, secret=access_key_secret, region_id=region_id)
        return client