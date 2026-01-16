using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Loom3Approach : MonoBehaviour
{

    public LightStand[] lights;
    public ParticleSystem[] fireballs;

    public TestAnimation[] myAnims;
    

    void OnTriggerEnter(Collider coll)
    {
        if (coll.CompareTag("Player"))
        {
            for (int i = 0; i < lights.Length; i++)
            {
                GameObject lightObj = lights[i].myLight;
                LightIntensityFlicker lightController = lightObj.GetComponent<LightIntensityFlicker>();
                lightObj.SetActive(true);
                lightController.StartCoroutine(lights[i].myLight.GetComponent<LightIntensityFlicker>().DieDown());
            }

            for (int i = 0; i < fireballs.Length; i++)
            {
                fireballs[i].Play();
            }

            for (int i = 0; i < myAnims.Length; i++)
            {
                myAnims[i].StartCoroutine(myAnims[i].startAnimation());
            }
            
            Destroy(transform.gameObject);
        }
    }
}