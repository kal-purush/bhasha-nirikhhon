using System.Collections;
using System.Collections.Generic;
using UnityEngine;
public class BulletBehavior : MonoBehaviour
{

    float power;
    Vector3 direction;
    float speed;

    bool setup;
    void Update()
    {
       if(setup){
           transform.Translate(direction*speed*Time.deltaTime);
       } 
    }

    public void Setup(Vector3 direction, float speed, float power, float lifeTime){
        this.direction = direction;
        this.speed = speed;
        this.power = power;

        setup = true;
        Invoke("Vanish",lifeTime);
    }

    private void Vanish(){
        Destroy(gameObject);
    }
}