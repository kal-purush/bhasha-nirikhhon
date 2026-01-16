using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.Events;

public class HandScannerTouchPad : MonoBehaviour
{
    [Tooltip("The tag of the direct interaction controllers")]public string triggerTag;
    
    [SerializeField] bool triggerEntered;
    [SerializeField] bool isProgressive;
    [SerializeField] bool coroutineStarted = false;
    [SerializeField] bool progressEventStarted = false;
    [Space(10)]
    public bool progressResets;
    public float progressionTime;
    
    float timeProgressed = 0;

    [SerializeField] float progressFromValue = 0;
    [SerializeField] float progressionValue = 0;
    [SerializeField] float progressToValue = 100;
    [Space(10)]
    public bool scanComplete = false;
    [Space(10)]
    public UnityEvent OnTouch;
    public UnityEvent OnTouchRelease;
    public UnityEvent OnTouchProgressive;

    public void OnTriggerEnter(Collider other)
    {
        if (other.CompareTag(triggerTag))
        {
            if (OnTouch != null) OnTouch.Invoke();
            triggerEntered = true;
        }
    }
    public void OnTriggerExit(Collider other)
    {
        if (other.CompareTag(triggerTag))
        {
            if (OnTouchRelease != null) OnTouchRelease.Invoke();
            triggerEntered = false;

            if (progressResets)
            {
                progressionValue = 0;
                timeProgressed = 0;
            }
        }
    }




    void Update()
    {
        //Check if Progressive event exists
        if (OnTouchProgressive != null)
        {
            isProgressive = true;
        }
        else
        {
            isProgressive = false;
        }

        //If there is a progressive event, and the coroutine has not started, start coroutine.
        if (isProgressive == true && coroutineStarted == false)
        {
            StartCoroutine(Progression());
            coroutineStarted = true;
        }

        //Invoke Progressive Event if Scan is complete
        if (scanComplete == true && progressEventStarted == false)
        {
            progressEventStarted = true;
            OnTouchProgressive.Invoke();
        }
    }

    IEnumerator Progression()
    {
        //Halts Progression if hand is not being scanned, or if scanning is complete
        while (!triggerEntered || scanComplete)
        {
            yield return null;
        }

        //Interpolates progressionvalue between FromValue and ToValue over progressionTime
        while (timeProgressed < progressionTime)
        {
            if (triggerEntered)
            {
                progressionValue = Mathf.Lerp(progressFromValue, progressToValue, timeProgressed / progressionTime);
                timeProgressed += Time.deltaTime;

            }

            yield return null;
        }

        progressionValue = progressToValue;
        scanComplete = true;
    }
}