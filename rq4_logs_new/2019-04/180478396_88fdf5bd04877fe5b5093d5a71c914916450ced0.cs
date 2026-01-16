////////////////////////////////////////////////////////////////////////
//
// Copyright (c) 2018 Audiokinetic Inc. / All Rights Reserved
//
////////////////////////////////////////////////////////////////////////

using UnityEngine;
using System.Collections;

//[RequireComponent(typeof(Collider))]
public class EventPositionConfiner : MonoBehaviour
{
    [Header("Event to clamp to AkAudioListener")]
    public AK.Wwise.Event Event = new AK.Wwise.Event();
    public AK.Wwise.Event StopEvent = new AK.Wwise.Event();

    [Header("Settings")]
    public float UpdateInterval = 0.05f;

    public bool ColliderInParent;
    
    #region private variables
    private IEnumerator positionClamperRoutine;

    private Collider trigger;
    private Transform targetTransform;

    private GameObject eventEmitter;
    #endregion

    private bool isWalking;

    private void Awake()
    {
        trigger = ColliderInParent ? GetComponentInParent<Collider>(): GetComponent<Collider>();
        trigger.isTrigger = true;

        eventEmitter = new GameObject("Clamped Emitter");
        eventEmitter.transform.parent = transform;
        Rigidbody RB = eventEmitter.AddComponent<Rigidbody>();
        RB.isKinematic = true;
        SphereCollider SPC = eventEmitter.AddComponent<SphereCollider>();
        SPC.isTrigger = true;
        eventEmitter.AddComponent<AkGameObj>();

        isWalking = false;
    }

    private void OnEnable()
    {
        var listenerGameObject = FindObjectOfType<AkAudioListener>();

        if (listenerGameObject != null)
        {
            targetTransform = listenerGameObject.transform;
        }
        else
        {
            Debug.LogError(this + ": No GameObject with 'AkAudioListener' Component found! Aborting.");
            enabled = false;
        }

        

        positionClamperRoutine = ClampEmitterPosition();
        StartCoroutine(positionClamperRoutine);
    }

    private void OnDisable()
    {
        Event.Stop(eventEmitter);

        if(positionClamperRoutine != null)
        {
            StopCoroutine(positionClamperRoutine);
        }
    }

    IEnumerator ClampEmitterPosition()
    {
        yield return new WaitForSeconds(2);
        
        while (true)
        {
            Vector3 closestPoint = trigger.ClosestPoint(targetTransform.position);
            eventEmitter.transform.position = closestPoint;
            

            if (Input.GetKey(KeyCode.W) && !isWalking)
            {
                Event.Post(eventEmitter);
                isWalking = true;
            }
        

            if (Input.GetKeyUp(KeyCode.W) && isWalking)
            {
                StopEvent.Post(eventEmitter);
                isWalking = false;
            }

            yield return new WaitForSecondsRealtime(UpdateInterval);
        }
    }

    private void OnDrawGizmos()
    {
        Gizmos.color = Color.blue;
        if (eventEmitter != null)
        {
            Gizmos.DrawSphere(eventEmitter.transform.position, 0.2f);
        }
    }

}