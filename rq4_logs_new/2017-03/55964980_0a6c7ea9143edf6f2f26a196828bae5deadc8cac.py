import bpy

objects = bpy.data.objects
ops = bpy.ops
context = bpy.context
scene = context.scene
logic = ops.logic

ops.object.mode_set(mode='OBJECT')

fish = objects["fish"]
ice = objects["ice_cube"]        
snow = objects["snow_ball"]

def smooth(obj):
    ops.object.select_all(action="DESELECT")
    scene.objects.active = obj
    obj.select = True
    ops.object.shade_smooth()

for obj in [fish, ice, snow]:
    smooth(obj)

ops.wm.save_as_mainfile(filepath=bpy.path.abspath("//")+"items.blend")
ops.wm.quit_blender()