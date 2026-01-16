import os

class FileHandler:
    def __init__(self):
        pass


    def read_file(self, file_path):
        try:
            if os.path.exists(file_path):
                file_obj = open(file_path, "r")
                file_content = file_obj.read()
                file_obj.close()
                return file_content
            return None
        except Exception as e:
            print(str(e))
            raise Exception(str(e))


    def write_file(self, file_path, data):
        try:
            file_obj = open(file_path, "w")
            file_obj.write(data)
            file_obj.close()
        except Exception as e:
            print(str(e))
            raise Exception(str(e))