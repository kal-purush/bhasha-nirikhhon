using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class LogCanvasDoComeIn : MonoBehaviour
{
    Animator associatedAnimator;
    public float logAnimCooldown = 0f;
    public bool logActive = false;
    // Start is called before the first frame update
    void Start()
    {
        associatedAnimator = GetComponent<Animator>();
    }

    // Update is called once per frame
    void Update()
    {
        if (logAnimCooldown <= 0f && Input.GetButtonDown("Fire1") && !logActive)
        {
            logActive = true;
            associatedAnimator.SetTrigger("Log Active");
            logAnimCooldown = 1.5f;
            associatedAnimator.ResetTrigger("Log Inactive");
        }
        else if(logAnimCooldown <= 0f && Input.GetButtonDown("Fire1") && logActive)
        {
            logActive = false;
            associatedAnimator.SetTrigger("Log Inactive");
            logAnimCooldown = 1.5f;
            associatedAnimator.ResetTrigger("Log Active");
        }
        if(logAnimCooldown >= 0f)
        {
            logAnimCooldown -= Time.deltaTime;
        }
    }
}