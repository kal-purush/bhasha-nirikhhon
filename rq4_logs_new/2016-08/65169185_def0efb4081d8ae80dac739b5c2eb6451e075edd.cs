using UnityEngine;

public class Repulsion : MonoBehaviour {

    [HideInInspector] public Idle idle;
    [HideInInspector] public FollowParent followParent;
    [HideInInspector] public CycleWaypoints cycleWaypoints;
    public GameObject repulsorObject;
    public bool isFleeing;
    public float fleeDistance;
    private NavMeshAgent self;
    private Vector3 previousLocation;
    private float speed;


    void Start()
    {
        idle = GetComponent<Idle>();
        followParent = GetComponent<FollowParent>();
        cycleWaypoints = GetComponent<CycleWaypoints>();
        self = GetComponent<NavMeshAgent>();

        isFleeing = false;
        fleeDistance = 5.0f;
        speed = 5.0f;

        GetLocation();
    }


    void GetLocation()
    {
        //This is the location the object will return to after it is no longer fleeting.
        previousLocation = transform.position;
    }


    void Update()
    {
        if (Vector3.Distance(transform.position, repulsorObject.transform.position) < fleeDistance)
        {
            Flee();
        }
        if (Vector3.Distance(transform.position, repulsorObject.transform.position) >= fleeDistance)
        {
            ReturnHome();
        }
    }


    void Flee()
    {
        if (idle != null) { idle.isIdling = false; }
        isFleeing = true;
        self.ResetPath();

        //Look at player.
        Quaternion repulsorRotation = Quaternion.LookRotation(repulsorObject.transform.position - transform.position);
        transform.rotation = Quaternion.Slerp(transform.rotation, repulsorRotation, speed * Time.deltaTime);

        //Move backwards.
        transform.position += -transform.forward * speed * Time.deltaTime;
    }


    void ReturnHome()
    {
        if (cycleWaypoints == null && followParent == null)
        {
            //Return to original idle position
            self.SetDestination(previousLocation);
            AreWeThere();
        }
        else if (followParent != null)
        {
            if (followParent.leader != null)
            {
                //Return to parent
                self.SetDestination(followParent.leader.transform.position);
                followParent.amFollowing = true;
            }
            if (followParent.leader == null)
            {
                //If the object loses its parent, it will behave as a normal Idled object.
                self.SetDestination(previousLocation);
                AreWeThere();
            }
        }
        //If the target has waypoints assigned, it should continue as normal.
        else if (cycleWaypoints != null)
        {
            isFleeing = false;
        }
    }


    void AreWeThere()
    {
        if (self.remainingDistance <= .5f)
        {
            if (idle != null)
            {
                if (followParent == null)
                {
                    //Idle behavior may continue as normal.
                    idle.isIdling = true;
                }
                else if (followParent != null)
                {
                    //FollowParent and Idle are generally mutually exclusive, but they SHOULD be attached together in case the object loses its parent.
                    //FollowParent behaviors will always have priority over Idle.
                    idle.isIdling = false;
                }
            }
            isFleeing = false;
        }
    }
}