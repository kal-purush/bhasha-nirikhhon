using UnityEngine;

public class RailMover : MonoBehaviour {

    public GameObject mover;
    public GameObject[] waypoint;

    //private Rigidbody rb;
    //private Transform destination;
    //private Vector3 direction;
    private int i = 0;

    void Start()
    {
        //player = GameObject.FindGameObjectWithTag("Player");
        //rb = GetComponentInParent<Rigidbody>();
        //GetDirection();
    }

    void OnTriggerStay(Collider other)
    {
        other.transform.SetParent(mover.transform);
    }

    void OnTriggerExit(Collider other)
    {
        other.transform.SetParent(null);
    }

    void FixedUpdate()
    {
        //rb.MovePosition(transform.position + direction * Time.deltaTime);

        mover.transform.position = Vector3.Lerp(mover.transform.position, waypoint[i].transform.position, Time.deltaTime * 0.5f);

        if (Vector3.Distance(mover.transform.position, waypoint[i].transform.position) < .5f)
        {
            i++;

            if (i >= waypoint.Length)
            {
                i = 0;
            }

            //GetDirection();
        }
    }

    /* This section was for moving the platform via Rigidbody.MovePosition.
     * However, since the Platform is a Non-Kinematic Rigidbody,
     * it is supposed to be moved by its tranform, not physics forces.

    void GetDirection()
    {
        destination = waypoint[i].transform;
        direction = (destination.position - mover.transform.position).normalized;
    }
     */
}