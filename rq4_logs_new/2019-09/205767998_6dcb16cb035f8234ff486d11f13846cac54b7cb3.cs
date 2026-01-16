using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class powerUp : MonoBehaviour
{
  private float x;
    private float z;
    private bool rotateX;
    private float rotationSpeed;

    void Start()
    {
        x = 0.0f;
        z = 0.0f;
        rotateX = true;
        rotationSpeed = 75.0f;
    }

    void FixedUpdate()
    {
        
            x += Time.deltaTime * rotationSpeed;

        

        transform.localRotation = Quaternion.Euler(0, x, 0);
    }
    private void OnTriggerEnter(Collider other) {
        if(other.name == "aj"){
            if(this.name == "PowerUp"){
            Vector3 enano = new Vector3(.5f,.5f,.5f);
            Vector3 normal = new Vector3(1,1,1);
            if(other.transform.lossyScale != enano){

            other.transform.localScale = enano;
            }else {
                other.transform.localScale = normal;
            }

        }
        else if(this.name == "Speed"){
            move Smove =  other.GetComponent<move>();
            float actualSpeed = Smove.getSpeed();
            float extraSpeed = 20f;
             actualSpeed += extraSpeed;
            Smove.setSpeed(actualSpeed);
            StartCoroutine(esperar());
            actualSpeed -= extraSpeed;
            Smove.setSpeed(actualSpeed);
        }
   
   }
}

 IEnumerator  esperar(){

            yield return new WaitForSeconds(3f); 
            


    }
}