import bpy
import bmesh
import pickle

bpy.ops.object.mode_set(mode='OBJECT')
obj_list = []
for ob in bpy.data.objects:
    if ob not in obj_list:
        obj_list.append(ob)

terrain_list = []
        
def add_logic_bricks(obj_name, obj_suffix):
    terrain = bpy.data.objects[obj_name+obj_suffix]
    bpy.ops.logic.sensor_add(type="ALWAYS", object=obj_name+obj_suffix)
    bpy.ops.logic.controller_add(type="PYTHON", object=obj_name+obj_suffix)
    sensor = terrain.game.sensors[-1]
    controller = terrain.game.controllers[-1]
    controller.mode = "MODULE"
    controller.module = "scripts.terrain.terrain"+obj_suffix+"_main"
    sensor.link(controller)
    if obj_suffix == "_physics":
        bpy.ops.object.select_all(action="DESELECT")
        bpy.context.scene.objects.active = terrain
        terrain.select = True
        bpy.ops.object.game_property_new(name="barrier", type="BOOL")
        terrain.game.properties["barrier"].value = True
    bpy.ops.object.select_all(action="DESELECT")
        
def cut_terrain(obj_name, obj_length, obj_width, obj_suffix):
    if obj_length >= obj_width:
        sub_obj_length = int(obj_length/3)
        sub_obj_width = int(obj_width/2)
    else:
        sub_obj_length = int(obj_length/2)
        sub_obj_width = int(obj_width/3)
    terrain = bpy.data.objects[obj_name+obj_suffix]
    bpy.context.scene.objects.active = terrain
    terrain.select = True
    bpy.ops.object.origin_set(type='ORIGIN_GEOMETRY')
    terrain_loc_rot = [[terrain.location[0], terrain.location[1], terrain.location[2]], []]
    bpy.ops.object.select_all(action='DESELECT')
    cut_n = 0
    global obj_list
    if (obj_length > 9):
        for i in range(6):
            bpy.context.scene.objects.active = terrain
            terrain.select = True
            bpy.ops.object.mode_set(mode='EDIT')
            obj = bpy.context.object
            me = obj.data
            bm = bmesh.from_edit_mesh(me)
            bpy.ops.mesh.select_all(action='DESELECT')
            ########################
            #sub_obj_index = i+1
            for m in range(sub_obj_width):
                for n in range(sub_obj_length):
                    bm.faces.ensure_lookup_table()
                    bm.faces[m + n*(obj_width - cut_n)].select = True
                    bmesh.update_edit_mesh(me, True)
                    bm.faces.ensure_lookup_table()
            ########################
            bpy.ops.mesh.separate(type='SELECTED')
            if cut_n < obj_width - sub_obj_width:
                cut_n += sub_obj_width
            else:
                cut_n = 0
            ########################
            sub_obj_name = obj_name+"_"+str(i)
            bpy.ops.object.mode_set(mode='OBJECT')
            bpy.ops.object.select_all(action='DESELECT')
            for ob in bpy.data.objects:
                if ob not in obj_list:
                    ob.name = sub_obj_name+obj_suffix
                    obj_list.append(ob)
                    break
            ########################
            terrain_loc_rot[1].append(cut_terrain(sub_obj_name, sub_obj_length, sub_obj_width, obj_suffix))
            ########################
        bpy.ops.object.select_all(action='DESELECT')
        bpy.context.scene.objects.active = terrain
        terrain.select = True
        bpy.ops.object.delete()
        obj_list.remove(terrain)
    else:
        add_logic_bricks(obj_name, obj_suffix)
        terrain_loc_rot.append([terrain.rotation_euler[0], terrain.rotation_euler[1], terrain.rotation_euler[2]])
        terrain_loc_rot.append([terrain.scale[0], terrain.scale[1], terrain.scale[2]])
    bpy.ops.object.select_all(action='DESELECT')
    #terrain_list.append(terrain_loc_rot)
    return terrain_loc_rot

#terrain_list_x = ["0", "1", "2", "3"]
#terrain_list_y = ["0", "1", "2"]

bpy.ops.object.select_all(action='DESELECT')

#for t_l_x in range(4):
#    for t_l_y in range(3):
#        terrain_list.append(cut_terrain("terrain_"+str(t_l_x)+"_"+str(t_l_y), 81, ""))

terrain_list = cut_terrain("terrain", 216, 216, "")
pickle.dump(terrain_list, open(bpy.path.abspath("//")+"terrain_loc_rot.p", "wb"))

#for t_l_x in range(4):
#    for t_l_y in range(3):
#        cut_terrain("terrain_"+str(t_l_x)+"_"+str(t_l_y), 81, "_physics")
cut_terrain("terrain", 216, 216, "_physics")

