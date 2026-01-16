using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.Video; 

public class RemoteControl : MonoBehaviour
{

    public VideoPlayer vid; 
    public VideoClip Teletubbies; 
    public VideoClip SesameStreet;
    public VideoClip Dora; 
    // Start is called before the first frame update
    void Start()
    {
        
    }

    // Update is called once per frame
    void Update()
    {
        if (Input.GetKeyDown(KeyCode.RightArrow)) { 
            print("right"); 
            vid.clip = SesameStreet; 
        }

        if (Input.GetKeyDown(KeyCode.LeftArrow)){ 
            print("left"); 
            vid.clip = Teletubbies; 
        }

        if (Input.GetKeyDown(KeyCode.UpArrow)){ 
            print("up"); 
            vid.clip = Dora; 
        }
    }
}