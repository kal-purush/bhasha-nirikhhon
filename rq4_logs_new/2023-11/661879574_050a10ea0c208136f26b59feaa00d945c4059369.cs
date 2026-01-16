using System.Collections.Generic;
using System.Linq;
using UnityEngine;

namespace ReupVirtualTwin.helpers
{
    public static class MaterialsHelper
    {
        //private List<GameObject> selectedObjects;
        //private int[] selectedMaterialIndexes;


        //public void SelectObjects(List<GameObject> objs, int[] objectsMaterialIndexes)
        //{
        //    selectedObjects = objs;
        //    selectedMaterialIndexes = objectsMaterialIndexes;
        //}

        public static void SetNewMaterial(List<GameObject> objs, int[] objectsMaterialIndexes, Material material)
        {
            foreach (var (obj, i) in objs.Select((v, i) => (v, i)))
            {
                var materials = obj.GetComponent<Renderer>().materials;
                Material[] newMaterials = new Material[materials.Length];
                newMaterials = materials;
                newMaterials[objectsMaterialIndexes[i]] = material;
                obj.GetComponent<Renderer>().materials = newMaterials;
            }

        }
    }
}