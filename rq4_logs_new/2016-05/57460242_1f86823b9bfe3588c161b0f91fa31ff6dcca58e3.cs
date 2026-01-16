using UnityEngine;
using System.Collections;

public class FallingDeath : MonoBehaviour
{

    Transform[] transforms;
    float dropTime;
    public Transform target;
    bool dropped = false;
    bool rising = false;
    float distance;
    float distCovered;
    public float speed = 1.0f;
    float fracJourney;
    Vector3[] starting;
    // Use this for initialization
    void Start()
    {

        transforms = gameObject.GetComponentsInChildren<Transform>();
        starting = new Vector3[transforms.Length];
        for(int i = 0; i < transforms.Length; i++)
            starting[i] = transforms[i].position;
        distance = Vector3.Distance(target.position, transform.position);
    }

    void Update()
    {
        if (dropped)
        {
            int j = 0;
            foreach (Transform i in transforms)
            {
                distCovered = (Time.time - dropTime) * speed;

                if (speed != 0)
                    fracJourney = distCovered / distance;

                i.transform.position = Vector3.MoveTowards(new Vector2(i.transform.position.x, starting[j].y), new Vector2(i.transform.position.x, target.position.y), fracJourney);
                j++;
            }
            if (transform.position.y == target.position.y)
            {
                dropped = false;
                rising = true;
                dropTime = Time.time;
            }
        }
        else if (rising)
        {
            int j = 0;
            foreach (Transform i in transforms)
            {
                j++;
                distCovered = (Time.time - dropTime) * speed;
                fracJourney = distCovered / distance;
                i.transform.position = Vector3.MoveTowards(new Vector2(i.transform.position.x, target.position.y), new Vector2(i.transform.position.x, starting[j].y), fracJourney);
            }
            if (transform.position.y == starting[j].y)
            {
                rising = false;
                dropped = false;
            }
        }


    }

    void OnTriggerEnter2D(Collider2D other)
    {
        if (other.name == "Player" && dropped != true && rising != true)
        {
            dropTime = Time.time;
            dropped = true;
            rising = false;
        }
    }
}