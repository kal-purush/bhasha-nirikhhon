using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class deerAnimation : MonoBehaviour
{
    float currentTime;
    float startingTime;
    bool isSleep;

    Animator anim;

    void Start()
    {
        anim = GetComponent<Animator>();
        currentTime = startingTime;    
    }

    // Update is called once per frame
    void Update()
    {
        startingTime = Random.Range(8f, 13f);
        currentTime -= 1 * Time.deltaTime;
        print(currentTime);

        if (isSleep == false && currentTime <= 0)
        {
            currentTime = startingTime;
            isSleep = true;
            anim.SetBool("sleep", true);
            
        }
        if (isSleep == true && currentTime <= 0)
        {
            currentTime = startingTime;
            isSleep = false;
            anim.SetBool("sleep", false);
            
        }
    }
}