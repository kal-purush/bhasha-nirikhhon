using UnityEngine;
using ReupVirtualTwin.helpers;
using ReupVirtualTwin.models;

namespace ReupVirtualTwin.behaviours
{
    public class HideWebMaterials : SelectObject
    {
        IWebMaterialContainerHandler _webMaterialContainerHandler;

        protected override void Start()
        {
            base.Start();
            _webMaterialContainerHandler = ObjectFinder.FindextensionsTriggers().GetComponent<IWebMaterialContainerHandler>();
        }

        public override void HandleObject(GameObject obj)
        {
            throw new System.Exception($"No object should be selected in {this.name}, {obj.name} was selected");
        }

        public override void MissObject()
        {
            _webMaterialContainerHandler.HideContainer();
        }

    }
}