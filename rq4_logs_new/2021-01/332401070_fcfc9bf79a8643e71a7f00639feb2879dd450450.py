import os 

class SoundboardConfig(object):
    dir_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    # path to local db
    database_path = f"{dir_path}\\database\\sounds.db"

    sound_files_path = f"{dir_path}\\soundfiles"