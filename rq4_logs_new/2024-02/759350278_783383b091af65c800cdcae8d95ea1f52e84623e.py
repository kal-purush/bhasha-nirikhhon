bl_info = {
    "name": "Dematerial",
    "author": "thelazyone",
    "version": (1, 0),
    "blender": (4, 0, 0),
    "location": "View3D > Sidebar > Lazy Tools",
    "description": "Deletes all materials from selected objects",
    "warning": "",
    "wiki_url": "",
    "category": "Mesh",
}

import bpy
from bpy.types import Operator, Panel

class MATERIAL_OT_delete_materials(Operator):
    bl_idname = "material.delete_materials"
    bl_label = "Delete Materials"
    bl_description = "Deletes all materials from selected objects"
    bl_options = {'REGISTER', 'UNDO'}

    @classmethod
    def poll(cls, context):
        return context.selected_objects is not None
        
    def execute(self, context):
        selected_objects = context.selected_objects
        for obj in selected_objects:
            if obj.type == 'MESH':
                # Clear material slots
                obj.data.materials.clear()
                
        self.report({'INFO'}, "Deleted materials from selected objects")
        return {'FINISHED'}

class MATERIAL_PT_delete_materials(Panel):
    bl_label = "Delete Materials"
    bl_idname = "MATERIAL_PT_delete_materials"
    bl_space_type = 'VIEW_3D'
    bl_region_type = 'UI'
    bl_category = 'Lazy Tools'
    bl_context = "objectmode"

    def draw(self, context):
        layout = self.layout
        layout.operator(MATERIAL_OT_delete_materials.bl_idname)

def register():
    bpy.utils.register_class(MATERIAL_OT_delete_materials)
    bpy.utils.register_class(MATERIAL_PT_delete_materials)

def unregister():
    bpy.utils.unregister_class(MATERIAL_OT_delete_materials)
    bpy.utils.unregister_class(MATERIAL_PT_delete_materials)

if __name__ == "__main__":
    register()