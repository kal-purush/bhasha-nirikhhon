from user_setting import get_photo_path, get_project_name
from os.path import join

class ImageName(object):

    def __init__(self):
        self.prefix = "%s_image"
        self.number = "%04d"
        self.file_type = ".jpg"

    def get_name_format(self):
        return self.prefix + self.number + self.file_type

    def instantiate_format(self,prefix, number, file_type):

        return prefix + number + file_type

    def get_image_name(self, project_name = "", number = -999, directory_path = "" ):
        '''If given the project name, then it will use the project name. If not,
        it will use the project name in user setting. If number is not given, then
        it will return the format. If given the directory_path, then the method
        will return a absolute path.
        '''

        if project_name == "":
            project_name = get_project_name()
        prefix = self.prefix%project_name

        if number == -999:
            number = self.number
        else:
            number = self.number%number

        image_name = self.instantiate_format(prefix, number, self.file_type)
        if directory_path != "":
            image_name = join(directory_path, image_name)
        return image_name

