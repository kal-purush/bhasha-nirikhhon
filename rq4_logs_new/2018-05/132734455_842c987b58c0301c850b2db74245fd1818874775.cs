using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class AuriculaireControl : MonoBehaviour {

    GameObject Auriculaire1;
    GameObject Auriculaire2;
    GameObject Auriculaire3;
    GameObject Auriculaire4;
    float degree;


    // Use this for initialization
    void Start()
    {
        degree = 0;
        Auriculaire1 = GameObject.FindGameObjectWithTag("Auriculaire1");
        Auriculaire2 = GameObject.FindGameObjectWithTag("Auriculaire2");
        Auriculaire3 = GameObject.FindGameObjectWithTag("Auriculaire3");
        Auriculaire4 = GameObject.FindGameObjectWithTag("Auriculaire4");

    }

    // Update is called once per frame
    void Update()
    {

    }

    public void CloseFinger(){
        if (degree < 30){
            Auriculaire1.transform.Rotate(Vector3.up, Time.deltaTime * 10);
            degree += Time.deltaTime * 10;
        }
        if (degree >= 30 && degree < 75) 
        {
            Auriculaire2.transform.Rotate(Vector3.up, Time.deltaTime * 10);
            degree += Time.deltaTime * 10;
        }
        if (degree >= 75 && degree < 120)
        {
            Auriculaire3.transform.Rotate(Vector3.up, Time.deltaTime * 10);
            degree += Time.deltaTime * 10;
        }
        if (degree >= 120 && degree < 165)
        {
            Auriculaire4.transform.Rotate(Vector3.up, Time.deltaTime * 10);
            degree += Time.deltaTime * 10;
        }
    }

    public void OpenFinger(){
        if (degree >0 && degree <=30)
        {
            Auriculaire1.transform.Rotate(Vector3.down, Time.deltaTime * 10);
            degree -= Time.deltaTime * 10;
        }
        if (degree > 30 && degree <= 75)
        {
            Auriculaire2.transform.Rotate(Vector3.down, Time.deltaTime * 10);
            degree -= Time.deltaTime * 10;
        }
        if (degree > 75 && degree <= 120)
        {
            Auriculaire3.transform.Rotate(Vector3.down, Time.deltaTime * 10);
            degree -= Time.deltaTime * 10;
        }
        if (degree > 120)
        {
            Auriculaire4.transform.Rotate(Vector3.down, Time.deltaTime * 10);
            degree -= Time.deltaTime * 10;
        }
    }
}