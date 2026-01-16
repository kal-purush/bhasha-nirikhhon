using UnityEngine;
using ReupVirtualTwin.models;

namespace ReupVirtualTwin.behaviours
{
    public class SelectWebMaterialSelectionTrigger : SelectObject
    {
        public override void HandleObject(GameObject triggerObject)
        {
            var trigger = triggerObject.GetComponent<IWebMaterialSelectionTrigger>();
            CreateContainer(trigger);
            SendCreateWebContainerMessage(trigger);
        }
        void CreateContainer(IWebMaterialSelectionTrigger trigger)
        {
            trigger.CreateContainer();
        }
        void SendCreateWebContainerMessage(IWebMaterialSelectionTrigger trigger)
        {
            var createWebMaterialsContainerMessage = trigger.GetWebContainerMessage();
            WebMessagesSender.SendWebMessage(createWebMaterialsContainerMessage);
        }
    }
}