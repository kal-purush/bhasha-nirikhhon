using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Plank : MonoBehaviour
{
    private Rigidbody rb;    
    private Collider[] plankColliders;

    [Header("Shake variables")]
    [SerializeField] private float shakeTime = 0.5f;
    private float currentTime = 0f;
    [SerializeField] private float shakeIntensity = 0.02f;
    private bool shakeObjects;

    [Header("Hammer variables")]
    private Vector3 hammerStart;
    private Quaternion hammerOrigRotation;
    [SerializeField] GameObject hammer;


    void Start()
    {
        //Initialize rigidbody for the plank and set hammer origin state for scene repetition purposes
        rb = GetComponent<Rigidbody>();
        shakeObjects = false;
        hammerStart = hammer.transform.position;
        hammerOrigRotation = hammer.transform.rotation;
    }


    void FixedUpdate()
    {
        //If the hammer has collided with the plank and there are shakeable objects currently on the plank the shake those objects
        if (shakeObjects && plankColliders != null)
        {
            //call ShakeObject() function for each object on the plank
            foreach (Collider hits in plankColliders)
            {
                ShakeObject(hits);
            }

            //Each object will shake for a small amount of time that can be edited
            currentTime += Time.deltaTime;

            //Once specified time is elapsed, stop shaking objects and reset the hammer position
            if (currentTime >= shakeTime)
            {
                shakeObjects = false;
                hammer.transform.position = hammerStart;
                hammer.transform.rotation = hammerOrigRotation;
                hammer.GetComponent<Rigidbody>().velocity = Vector3.zero;
                currentTime = 0f;
            }
        }
    }

    //Detects if the hammer has hit this plank, then calls Physics OverlapBox to generate a list of objects that are
    //on the plank (excluding the hammer)
    private void OnCollisionEnter(Collision collision)
    {
        if (collision.gameObject.layer.Equals(LayerMask.NameToLayer("Hammer")))
        {
            //Layer mask for shakeable objects
            int layerMask = LayerMask.GetMask("Things");
            //populate list of objects on the plank
            plankColliders = Physics.OverlapBox(rb.GetComponent<BoxCollider>().bounds.center, rb.GetComponent<BoxCollider>().bounds.size/2, rb.GetComponent<BoxCollider>().transform.rotation, layerMask);
            hammer.GetComponent<Rigidbody>().velocity = Vector3.zero;

            //Boolean to allow objects to start shaking
            shakeObjects = true;
            
        }
    }

    //Shakes object by generating a random offset for each position component of the object
    private void ShakeObject(Collider thing)
    {
           Vector3 originalPosition = thing.gameObject.transform.localPosition;
           thing.gameObject.transform.localPosition = new Vector3(originalPosition.x + Random.Range(-shakeIntensity,shakeIntensity), originalPosition.y + Random.Range(-shakeIntensity, shakeIntensity), originalPosition.z + Random.Range(-shakeIntensity, shakeIntensity));

    }
}