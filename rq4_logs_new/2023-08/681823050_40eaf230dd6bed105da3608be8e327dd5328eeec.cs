using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public enum states{
    patrol,
    chase,
    evade
}

public class Behaviour : MonoBehaviour
{
    public states state = states.patrol;

    // Movement variables
    public float speed = 1.0f;
    public float rotationSpeed = 1.0f;
    private float rotationInterval = 2.0f;
    private float nextRotationTime;

    // Detection variables
    public float maxDetectionDistance = 20f;
    public float fov = 90f;
    private float nRays = 20;

    public int boxDistance = 20;

    List<Ray> rays = new List<Ray>();
    private GameObject target;

    void Start()
    {
        nextRotationTime = Time.time + rotationInterval;
    }

    void Boundaries(){
        if (transform.position.x > boxDistance){
            transform.position = new Vector3(boxDistance, transform.position.y, transform.position.z);
            transform.eulerAngles = new Vector3(0, transform.eulerAngles.y + 180, 0);
        }
        else if (transform.position.x < -boxDistance){
            transform.position = new Vector3(-boxDistance, transform.position.y, transform.position.z);
            transform.eulerAngles = new Vector3(0, transform.eulerAngles.y + 180, 0);

        }
        if (transform.position.z > boxDistance){
            transform.position = new Vector3(transform.position.x, transform.position.y,boxDistance);
            transform.eulerAngles = new Vector3(0, transform.eulerAngles.y + 180, 0);

        }
        else if (transform.position.z < -boxDistance){
            transform.position = new Vector3(transform.position.x, transform.position.y, -boxDistance);
            transform.eulerAngles = new Vector3(0, transform.eulerAngles.y + 180, 0);

        }
    }

    private void RotateEntity()
    {
        // Generate a random rotation angle
        float randomAngle = Random.Range(0.0f, 360.0f);

        // Rotate around the y-axis by the random angle
        transform.rotation = Quaternion.Slerp(transform.rotation, Quaternion.Euler(0.0f, randomAngle, 0.0f), rotationSpeed * Time.deltaTime);
    }

    void Patrol(){
        if (Time.time >= nextRotationTime){
            RotateEntity();
            nextRotationTime = Time.time + rotationInterval;
            rotationInterval = Random.Range(1.0f, 3.0f);
        }

        transform.Translate(Vector3.forward * speed * Time.deltaTime);
    }

    void Chase(){
        if (target != null){
            transform.position = Vector3.MoveTowards(transform.position, target.transform.position, Time.deltaTime * speed);
            transform.LookAt(target.transform.position);

            if (Vector3.Distance(transform.position, target.transform.position) < 2.0f){
                Debug.Log("Comiendo");
                Destroy(target);
                Debug.Log("Devoro");
            }
        }
        else{
            Patrol();
        }
    }

    void Update()
    {
        rays.Clear();
        for (int i = 0; i < nRays; i++) // For loop to create multiple raycasts
        {
            float angle = i / nRays * fov - fov / 2; // Calculate the angle of the raycast
            Quaternion rot = Quaternion.AngleAxis(angle, transform.up); // Calculate the rotation of the raycast
            Vector3 dir = rot * transform.forward; // Calculate the direction of the raycast
            Ray ray = new Ray(transform.position, dir); // Create the raycast
            rays.Add(ray);
            Debug.DrawRay(transform.position, dir * maxDetectionDistance, Color.red); // Draw the raycast in the editor
        }

        state= states.patrol;

        foreach (Ray ray in rays)
        {
            RaycastHit hit;
            if (Physics.Raycast(ray, out hit, maxDetectionDistance))
            {
                if (hit.collider.tag == "prey")
                {
                    target = hit.collider.gameObject;
                    state = states.chase;

                    target.GetComponent<Herbivore>().state = statesHerbivore.evade;
                    
                    target.GetComponent<Herbivore>().predator = gameObject;
                }       
            }
        }
        Boundaries();
        if (state == states.patrol){
            Patrol();
        }
        else if (state == states.chase){
            Chase();
        }
    }
}