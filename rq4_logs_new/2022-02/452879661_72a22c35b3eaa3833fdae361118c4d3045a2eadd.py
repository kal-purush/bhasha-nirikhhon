from unittest import loader


from lib import loader
class Assets(object):
     def __init__(self, asset_path, asset_type):
         self.asset_path = asset_path
         self.asset_type = asset_type


     def load_resource(self, volume=None):
         return loader.load_asset(self.asset_path, self.asset_type, volume)