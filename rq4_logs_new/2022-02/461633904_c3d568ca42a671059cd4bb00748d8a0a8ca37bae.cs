using System;
using UnityEngine;

namespace UnityTemplateProjects
{
    public class Chest : MonoBehaviour
    {
        [Header("Chest Properties")]
        public Material trimMaterial;
        public Material chestMaterial;
        public Material lockMaterial;
        public String owner;
        public Boolean hasRune;
        public Boolean isMimic;
        
        [Header("Chest Renderers")]
        [SerializeField] private GameObject chestObject;
        [SerializeField] private GameObject trimObject;
        
        [Header("Lock Renderers")]
        [SerializeField] private GameObject lockTrimObject;
        [SerializeField] private MeshRenderer lockBase;

        public void ReskinChest(Material chestMat, Material trimMat, Material lockMat, Material lockTrimMat, String ownerStr, bool rune)
        {
            foreach (var obj in chestObject.GetComponentsInChildren<MeshRenderer>())
            {
                obj.material = chestMat;
            }
            
            foreach (var obj in trimObject.GetComponentsInChildren<MeshRenderer>())
            {
                obj.material = trimMat;
            }

            foreach (var obj in lockTrimObject.GetComponentsInChildren<MeshRenderer>())
            {
                obj.material = lockTrimMat;
            }

            chestMaterial = chestMat;
            trimMaterial = trimMat;
            lockMaterial = lockMat;
            lockBase.material = lockMat;
            owner = ownerStr;
            hasRune = rune;
        }
        
        
    }
}