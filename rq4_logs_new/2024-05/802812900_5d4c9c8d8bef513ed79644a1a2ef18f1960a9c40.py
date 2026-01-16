import sys
sys.dont_write_bytecode = True

from harbor3d import Dock, Shipwright
import os

from harbor3d.util.bone_json_util import PostureWrapper
from harbor3d.util.json_util import JsonLoader

sys.path.append(os.sep.join(os.path.dirname(os.path.abspath(__file__)).split(os.sep)[:-1]))
from path_info import Const as PathInfo
from spec_model import Const

def main():
    path = os.path.dirname(os.path.abspath(__file__))
    posture = os.path.join(PathInfo.dir_posture_json, PathInfo.file_posture_model_generated)
    fname = ''

    sw = Shipwright(Dock())

    json_loader = JsonLoader(posture)
    pw = PostureWrapper(json_loader.fetch())
    pw.remove_offset_all()
    pw.remove_rotation_all()

    objects, scale = sw.load_bones(pw)
    sw.load_submodules_name_match(objects, PathInfo.dirs_parts_assembly, Const.bones_alias)
    sw.generate_stl_binary(path, fname=fname, concatinated=False)

if __name__ == "__main__":
    main()