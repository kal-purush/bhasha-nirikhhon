using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;
using System.IO;


public class CaptureManager : MonoBehaviour
{
    public Text captureText;
    public string capture_text_buffer;
    int capture_timer = 0;
    int record_timer = 0;
    bool recording = false;
    int processing_state = 0;

    // Use this for initialization
    void Start()
    {
        VRCapture.Instance.RegisterSessionCompleteDelegate(HandleRecordFinish);
        Application.runInBackground = true;
        VRCapture.Instance.GetCaptureVideo(0).isEnabled = true;
        VRCapture.Instance.GetCaptureAudio().isEnabled = false;
    }

    // Update is called once per frame
    void Update()
    {

        if (capture_timer == 199) { captureText.text = capture_text_buffer;}//wait a frame before displaying capture text.
        if (capture_timer > 0) { capture_timer--; }
        else if (recording)
        {
            record_timer++;
            captureText.text = "Recording";
            for (int i = 0; i < (record_timer / 30) % 3; i++)
            {
                captureText.text += ".";
            }
        }
        else { captureText.text = ""; }
        if (Input.GetKeyDown(KeyCode.F11) && capture_timer==0)
        {
            Directory.CreateDirectory(VRCaptureConfig.SaveFolder);
            string filename = VRCaptureConfig.SaveFolder + "screenshot_" + System.DateTime.Now.ToString("MMdd_HH_mm") + ".png";
            Application.CaptureScreenshot(filename);
            capture_text_buffer = "Writing screenshot " + filename;
            capture_timer = 200;
        }
        if (Input.GetKeyDown(KeyCode.R) && capture_timer == 0)
        {
            if (recording == false)
            {
                VRCapture.Instance.BeginCaptureSession();
                Debug.Log("Recording...");
                recording = true;
                record_timer = 0;
            }
            else {
                VRCapture.Instance.EndCaptureSession();
                Debug.Log("End recording, processing...");
                recording = false;
                processing_state = 2;
                capture_timer = 200;
                capture_text_buffer = "Wrote replay to " + VRCaptureConfig.SaveFolder;// + filename;
            }
        }
        if (recording==false && processing_state == 2)
        {
            Debug.Log("Video processed!");
            processing_state = 0;
        }
        if (Input.GetKeyDown(KeyCode.V) && (Directory.Exists(VRCaptureConfig.SaveFolder))){
            Debug.Log("Opening "+ VRCaptureConfig.SaveFolder);
            System.Diagnostics.Process.Start(VRCaptureConfig.SaveFolder);
        }
    }

    void HandleRecordFinish(){processing_state = 1;}
}