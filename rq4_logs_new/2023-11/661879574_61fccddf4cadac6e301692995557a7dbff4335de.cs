using System.Collections.Generic;
using UnityEngine;
using ReupVirtualTwin.helpers;

namespace ReupVirtualTwin.models
{
    //public class WebMaterialSelectionTrigger : MonoBehaviour, IMaterialSelectionTrigger
    public class WebMaterialSelectionTrigger : MonoBehaviour
    {
        public List<Material> selectableMaterials;
        public List<GameObject> objects;
        [HideInInspector]
        public int[] objectsMaterialIndexes;

        //IWebMaterialContainerHandler _containerHandler;

        private void Start()
        {
            //_containerHandler = ObjectFinder.FindMaterialsContainerHandler().GetComponent<IWebMaterialContainerHandler>();
        }

        //public GameObject CreateContainer()
        //{
        //    return _containerHandler.CreateContainer(this);
        //}
    }
}