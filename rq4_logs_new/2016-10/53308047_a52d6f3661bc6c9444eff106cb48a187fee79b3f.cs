using UnityEngine;

namespace PlanetarySurfaceStructures
{
    class KPBSTransparendPodHelper : PartModule
    {
        //the name of the additional internal
        [KSPField]
        public string hiddenOverlayTransformName = string.Empty;

        //the transform for an additional depthmask
        private Transform hiddenOverlayTransform;

        /**
         * Called at the start. Initialize animation and state of this module
         */
        public override void OnStart(PartModule.StartState state)
        {
            if ((hiddenOverlayTransform == null) && (hiddenOverlayTransformName != null) && (hiddenOverlayTransformName != string.Empty))
            {
                hiddenOverlayTransform = part.internalModel.FindModelTransform(hiddenOverlayTransformName);
            }
        }


        /**
         * The update method of the module
         */
        public void Update()
        {
            if (!HighLogic.LoadedSceneIsFlight && !HighLogic.LoadedSceneIsEditor)
            {
                return;
            }
            
            //search for the transform (it seems to get lost when only loading it in onStart()
            if ((hiddenOverlayTransform == null) && (hiddenOverlayTransformName != null) && (hiddenOverlayTransformName != string.Empty))
            {
                hiddenOverlayTransform = part.internalModel.FindModelTransform(hiddenOverlayTransformName);
            }

            //update the state of the transform
            if (hiddenOverlayTransform != null)
            {
                if ((isStockOverlayActive()) && (hiddenOverlayTransform.gameObject.activeSelf))
                {
                    hiddenOverlayTransform.gameObject.SetActive(false);
                }
                else if ((!isStockOverlayActive()) && (!hiddenOverlayTransform.gameObject.activeSelf))
                {
                    hiddenOverlayTransform.gameObject.SetActive(true);
                }
            }
        }

        /**
         * When the module is destroyed
         */
        public void OnDestroy()
        {
            if (hiddenOverlayTransform != null)
            {
                hiddenOverlayTransform.gameObject.SetActive(true);
            }
        }

        //find a camera by its name
        private bool isStockOverlayActive()
        {
            int count = Camera.allCamerasCount;
            for (int i = 0; i < count; ++i)
            {
                //when the camera for the overly exists return true
                if (Camera.allCameras[i].name.Equals("InternalSpaceOverlay Host"))
                {
                    return true;
                }
            }
            return false;
        }
    }
}