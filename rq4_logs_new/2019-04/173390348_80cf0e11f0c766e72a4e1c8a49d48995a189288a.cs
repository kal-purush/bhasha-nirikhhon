using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ShirtSwap : MonoBehaviour
{
    //public Material Blue;
    public Material[] material;
    Renderer rend;
   
    void Start()
    {
        rend = GetComponent<Renderer>();
        rend.enabled = true;
        rend.sharedMaterial = material[0];
    }

   
     void OnCollisionEnter(Collision col){

        /*if (col.gameObject.CompareTag("Shirt")){
           
            this.gameObject.GetComponent<Renderer>().material = Blue;
            */
        if(col.gameObject.tag == "Shirt"){
            rend.sharedMaterial = material[1];
        }

        }
    }
