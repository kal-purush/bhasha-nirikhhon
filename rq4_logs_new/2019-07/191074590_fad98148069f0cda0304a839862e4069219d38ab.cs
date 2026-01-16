using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ForParticle : MonoBehaviour
{
    private float countTime;
    public GameObject particle2;
    // Start is called before the first frame update
    void Start()
    {
        countTime = 0;
        //particle2 = GetComponentInChildren<GameObject>().gameObject;
        particle2.SetActive(false);
    }

    // Update is called once per frame
    void Update()
    {
        countTime += Time.deltaTime;
        if(countTime>=0.6f)
        {
            particle2.SetActive(true);
        }
    }
}