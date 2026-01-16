using UnityEngine;
using System.Collections;

public class Player : MonoBehaviour {

    public AnimationClip Earth;
    public AnimationClip Lightning;
    public AnimationClip Fire;
    public AnimationClip Idle;

    public void Start(){
    }

    public void PlayEarth(){
        GetComponent<Animator>().SetTrigger("earth");
    }

    public void PlayLightning(){
        GetComponent<Animator>().SetTrigger("light");
    }

    public void PlayFire(){
        GetComponent<Animator>().SetTrigger("fire");
    }

}