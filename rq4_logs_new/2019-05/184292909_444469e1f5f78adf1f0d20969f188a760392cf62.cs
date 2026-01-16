using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class GunBehavior : MonoBehaviour
{

    enum GunState
    {
        idle,
        movingLeft,
        movingRight
    }

    [SerializeField][Range(0,10)]
    float rotateSpeed = 0;
    Transform planetTransform;
    GunState state;


    void Start()
    {
        planetTransform = transform.parent;
        state = GunState.idle;
    }

    // Update is called once per frame
    void Update()
    {
        if(state != GunState.idle){
            if(state == GunState.movingLeft){
                Rotate(rotateSpeed);
            }

            if(state == GunState.movingRight){
                Rotate(-rotateSpeed);
            }
        }
    }

    void Rotate(float speed){
        transform.RotateAround(planetTransform.transform.position, new Vector3(0,0,1), speed);
    }

    public void MoveToLeft(){
        state = GunState.movingLeft;
    }

    public void MoveToRight(){
        state = GunState.movingRight;
    }

    public void Stop(){
        state = GunState.idle;
    }

}