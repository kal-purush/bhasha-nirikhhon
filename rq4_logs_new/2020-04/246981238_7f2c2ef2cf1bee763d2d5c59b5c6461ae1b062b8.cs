using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class endLine : MonoBehaviour
{

    public Transform target;
    public GameObject explosion;

    public AudioSource clapping; // sound to be played when winning
    public AudioSource cheering; // sound to be played when winning

    public Text winningText;
    [SerializeField] private Text uiText;
    [SerializeField] private float mainTimer;

    Vector3 lastPos; // car last position
    public Transform car;
    float threshold = 0.1f; // minimum displacement to recognize.

    private float timer;
    private bool canCount = true;
    private bool doOnce = false;

    void Start()
    {
        timer = mainTimer;
        clapping.GetComponents<AudioSource>();
        cheering.GetComponents<AudioSource>();


        lastPos = car.position;
    }

    void Update()
    {
        Vector3 offset = car.position - lastPos;

        // start timer if car position changed
        if (offset.x > threshold || offset.x < -threshold)
        {
            // if timer > 0 count down //
            if (timer > 0.0f && canCount)
            {

                timer -= Time.deltaTime;
                uiText.text = timer.ToString("F");
            }
            // if timer reach 0 stop count down
            else if (timer <= 0.0f && !doOnce)
            {
                // display try again here ..


                //
                canCount = false;
                doOnce = true;
                uiText.text = "0.00";
                timer = 0.0f;
            }


        }
    }

    // when car hit end line 
    void OnCollisionEnter(Collision obj)
    {
        if (obj.gameObject.name == "Car")  // if Car reaches the end line
        {
            // play sound
            clapping.Play();
            cheering.Play();
            // stop time//
            canCount = false;


            // end game //


            //Instantiate(explosion, target.position, target.rotation);


            //Destroy(this.gameObject);  // To destroy the line
             //Destroy(obj.gameObject); // To destroy the car
        }

    }

}