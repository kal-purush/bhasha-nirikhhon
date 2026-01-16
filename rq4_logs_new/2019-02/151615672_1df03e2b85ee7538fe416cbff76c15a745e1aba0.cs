using UnityEngine;
using System.Collections;
using System.Collections.Generic;

 
public class Settings : MonoBehaviour {
    public void AdjustAmbientLight (float rbgValue){
        RenderSettings.ambientLight = new Color (rbgValue, rbgValue, rbgValue, 1);
    }
 
    public void AdjustVolume (float volume){
        AudioListener.volume = volume;
    }
}