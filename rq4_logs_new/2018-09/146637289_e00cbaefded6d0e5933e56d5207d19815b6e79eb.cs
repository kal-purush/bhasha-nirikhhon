using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class SimpleMovement : MonoBehaviour {
    public float speed = 0.1f;
    private Vector3 pos;
	void Start () {
        pos = this.transform.position;
	}
	
	void Update () {
        if(Input.GetKey(KeyCode.UpArrow)){
            pos.z += speed;
        }else if (Input.GetKey(KeyCode.DownArrow)){
            pos.z -= speed;
        }else if (Input.GetKey(KeyCode.LeftArrow)){
            pos.x -= speed;
        }else if (Input.GetKey(KeyCode.RightArrow)){
            pos.x += speed;
        }
        this.transform.position = pos;
	}
}