using UnityEngine;
using UnityEngine.UI;
using System.Collections;

public class RecordTimeScript : MonoBehaviour {

    // Use this for initialization
    bool isRecording;
    float recordTime;
	void Start () {
        Reset();
        Toggle();
	}
	
	// Update is called once per frame
	void Update () {
        if (isRecording)
        {
            this.recordTime += Time.deltaTime;
        }

        this.setUI();
	
	}

    void Toggle() {
        this.isRecording = !this.isRecording;
    }

    void Reset() {
        this.isRecording = false;
        this.recordTime = 0;
    }

    string toText() {
        int toSeconds = Mathf.FloorToInt(this.recordTime)%60;
        int getMiliseconds = Mathf.FloorToInt(this.recordTime * 1000)%1000;
        Debug.Log(toSeconds);
        int toMinutes = toSeconds/60;
        string sReturn = ("Record Time: " + toMinutes.ToString("D2") + ":" + toSeconds.ToString("D2") + "." + getMiliseconds.ToString("D3"));
        return sReturn;
    }

    void setUI() {
        this.GetComponent<Text>().text = this.toText();
    }
}