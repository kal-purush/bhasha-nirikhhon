using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class capsulaScriptFinal : MonoBehaviour
{
    public GameObject capsula;
    public GameObject capsula1;
    public GameObject capsula2;
    public bool esVerdadero;

    void FixedUpdate()
    {
        bool cajaEsVerdadero = capsula1.GetComponent<CapsulaScript>().esVerdadero;
        bool esferaEsVerdadero = capsula2.GetComponent<capsulaScriptOr>().esVerdadero;
        //  Color c = new Color(Random.value, Random.value, Random.value);
        // capsula.GetComponent<MeshRenderer>().material.color = c;
        if (cajaEsVerdadero && esferaEsVerdadero)
        {
            capsula.GetComponent<MeshRenderer>().material.color = Color.white;
            Debug.Log("Entro");
        }
        else
        {
            capsula.GetComponent<MeshRenderer>().material.color = Color.black;
        }

        esVerdadero = cajaEsVerdadero && esferaEsVerdadero;
    }
}