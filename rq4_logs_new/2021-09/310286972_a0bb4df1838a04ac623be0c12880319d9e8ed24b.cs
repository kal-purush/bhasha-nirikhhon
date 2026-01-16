using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class obile_HipStamp_M : MonoBehaviour
{
    public GameObject player, hipStamp, jump;
    public Vector3 pos;
    void Start()
    {
        
    }

    // Update is called once per frame
    void Update()
    {
        pos = player.transform.position;
        if (pos.y >= 0.5)
        {
            hipStamp.SetActive(true);
            jump.SetActive(false);
        }
        else
        {
            hipStamp.SetActive(false);
            jump.SetActive(true) ;
        }
    }
}