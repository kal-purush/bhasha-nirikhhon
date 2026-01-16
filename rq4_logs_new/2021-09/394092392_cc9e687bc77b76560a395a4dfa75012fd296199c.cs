using System.Collections;
using System.Collections.Generic;
using UnityEngine;

/*

    Class for managing all in-game cameras
        Ensure one camera used at a time
        Switch between camera views

 */
 namespace Cameras
{
    public class CameraManager : MonoBehaviour
    {
        [HideInInspector] public GameCamera[] cameras;
        int currentCam;
        public GameCamera defaultCamera;
        public GameCamera previousCamera, currentCamera;

        [Header("Shifting")]
        public float shiftDistance;
        public LayerMask shiftable;
        public bool shiftingAllowed;
        bool canShift;
        float shiftResetTimer = 0f, shiftReset = 1f;
        
        private void Awake()
        {
            cameras = FindObjectsOfType<GameCamera>();
        }

        // Start is called before the first frame update
        void Start()
        {
            // Ensure default camera is active and all others are inactive on start
            foreach (GameCamera cam in cameras)
            {
                if (cam == defaultCamera)
                    Enable(cam);
                else
                    Disable(cam);
            }
        }

        private void Update()
        {
            if (shiftingAllowed)
            {
                ShiftInput();

                ShiftReset();
            }
        }

        void ShiftInput()
        {
            if (canShift)
            {
                if (Input.GetKeyDown(KeyCode.RightShift) || Input.GetMouseButtonDown(0))
                {
                    if (currentCam < cameras.Length - 1)
                    {
                        currentCam++;
                    }
                    else
                    {
                        currentCam = 0;
                    }
                    Set(cameras[currentCam]);
                }

                if (Input.GetKeyDown(KeyCode.LeftShift)|| Input.GetMouseButtonDown(1))
                {
                    if (currentCam > 0)
                    {
                        currentCam--;
                    }
                    else
                    {
                        currentCam = cameras.Length - 1;
                    }
                    Set(cameras[currentCam]);
                }
            }
        }

        //enables as the Player 
        public void Enable(GameCamera camera)
        {
            //enable the obj
            camera.gameObject.SetActive(true);

            //fadeout 
            if(camera.shiftUI)
                camera.shiftUI.FadeOut();

            //set as current camera 
            currentCamera = camera;
        }

        //disables from being the Player 
        public void Disable(GameCamera camera)
        {
            //disable the obj
            camera.gameObject.SetActive(false);
        }

        public void Set(GameCamera camera)
        {
            if (camera == null)
                return;

            if (currentCamera != null)
                Disable(currentCamera);

            Enable(camera);

            //shift reset 
            shiftResetTimer = shiftReset;
            canShift = false;
        }

        public void Reset()
        {
            if (currentCamera != null && currentCamera != defaultCamera)
                Disable(currentCamera);

            Enable(defaultCamera);
        }

        //resets shift ability after it is used 
        void ShiftReset()
        {
            if (canShift == false)
            {
                shiftResetTimer -= Time.deltaTime;
                if (shiftResetTimer < 0)
                {
                    canShift = true;
                }
            }
        }
    }

}