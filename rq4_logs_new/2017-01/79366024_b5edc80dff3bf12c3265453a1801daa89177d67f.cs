using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

namespace VRCapture.PongCapture
{

    public class CaptureManager : MonoBehaviour
    {
        public Text captureText;
        public string capture_text_buffer;
        int capture_timer = 0;
        bool isProcessing = false;
        bool isDone = false;

        // Use this for initialization
        void Start()
        {
            VRCapture.Instance.RegisterSessionCompleteDelegate(HandleCaptureFinish);
            Application.runInBackground = true;
        }

        // Update is called once per frame
        void Update()
        {
            if (capture_timer == 199) { captureText.text = capture_text_buffer;}//wait a frame before displaying capture text.
            if (capture_timer > 0) { capture_timer--; }
            else { captureText.text = ""; }
            if (Input.GetKeyDown(KeyCode.F11) && capture_timer==0)
            {
                string filename = VRCaptureConfig.SaveFolder + "screenshot_" + System.DateTime.Now.ToString("MMdd_HH_mm") + ".png";
                Application.CaptureScreenshot(filename);
                capture_text_buffer = "Writing screenshot " + filename;
                capture_timer = 200;
            }
            if (Input.GetKeyDown(KeyCode.R) && capture_timer == 0)
            {
                if (isDone == false)
                {
                    
                    VRCapture.Instance.BeginCaptureSession();
                    Debug.Log("Recording...");
                    isDone = true;
                }
                else {
                    VRCapture.Instance.EndCaptureSession();
                    Debug.Log("End recording, processing...");
                    isDone = true;
                }
            }
            if (isProcessing && isDone)
            {
                Debug.Log("Video processed!");
                capture_text_buffer = "Wrote video " + VRCaptureConfig.SaveFolder;// + filename;
                capture_timer = 200;
                isProcessing = false;
            }
            if (Input.GetKeyDown(KeyCode.V)) {
                Debug.Log("Opening "+ VRCaptureConfig.SaveFolder);
                System.Diagnostics.Process.Start(VRCaptureConfig.SaveFolder);
            }
        }

        void HandleCaptureFinish(){isProcessing = true;}
    }
}