using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ButtonInstancer : MonoBehaviour
{
    private GameObject jumpButton;
    private GameObject slideButton;

    void Start(){
        jumpButton=Resources.Load("ButtonJump") as GameObject;
        slideButton=Resources.Load("ButtonSlide") as GameObject;
    
        Vector3 rotation=new Vector3(0,0,6);
        for(int i=0; i<60; i++){
            Instantiate(jumpButton, new Vector3(0,2,0), Quaternion.identity,transform);
            transform.Rotate(rotation,Space.Self);
        }
    }

}