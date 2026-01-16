using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Mudslide : MonoBehaviour {

    MP_Boost_Accel speed;


    // Use this for initialization
    void Start()
    {
        speed = GameObject.Find("Player").GetComponent<MP_Boost_Accel>();
    }

    // Update is called once per frame
    void Update()
    {

    }

    void OnTriggerEnter(Collider other)
    {
        if (other.gameObject.tag == "Player")
        {
            //SceneManager.LoadScene("Menu");
            Debug.Log("TEst");
            speed.ResetSpeed();

        }

    }
}