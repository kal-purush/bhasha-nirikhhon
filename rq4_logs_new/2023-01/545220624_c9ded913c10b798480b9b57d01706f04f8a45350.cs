using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using System.Linq;

public class RecordingTest : MonoBehaviour
{
    [SerializeField] AudioClip clip;
    float[] waveData_;
    // [SerializeField] float[] ClipData_ = new float[1024];
    [SerializeField] AudioSource aus;
    [SerializeField] AudioSource aus2;

    void Start()
    {
        //全データ分の配列を用意
        waveData_ = new float[aus.clip.samples * aus.clip.channels];
    }

    void Update()
    {
        AudioListener.GetOutputData(waveData_,1);
        var volume = waveData_.Select(x => x*x).Sum() / waveData_.Length;
        transform.localScale = Vector3.one * volume;
        Debug.Log(waveData_);

        // if(!aus.isPlaying){
        //     Debug.Log("終了");
        //     var audioClip = AudioClip.Create("clipNameTest", waveData_.Length, 1, 44100, false);
        //     audioClip.SetData(waveData_, 0);
        //     clip = audioClip;
        // }

        // if(Input.GetKeyDown(KeyCode.Tab)){
        //     Debug.Log("tab");
        //     aus2.clip = clip;
        //     aus2.Play();
        // }
    }
}