using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class SImpleStateMachine : MonoBehaviour
{
    /// <summary>
    /// This class has a few bugs in it and the basic structure of a one class
    /// State machine with a switch statement enum implementation.  It is only 
    /// here as an example of an innefficeint and inflexible way of making a state
    /// machine, I don't recommend this approach.  If you want for extra credit try 
    /// and get it working!
    /// </summary>
    public enum State
    {
        Sleep,
        Scout,
        Patrol,
        Chase,
        Reproduce, 
        Flee
    }

    public float scoutTimer = 3f;
    public float patrolTimer = 3f;
    public float reproduceTimer = 5f;
    public GameObject newPatrolPoint;
    public float randDestRange = 3f;
    public State currentState;
    public GameObject[] navPoints;
    public int navPointNum;
    public float remainingDistance;
    public Transform destination;
    public Renderer[] childrenRend;
    public GameObject[] objectsOfInterest;
    public float detectionRange = 4;
    public float fleeRange = 5;
    public float speed = 0.5f;
    public float accuracy = 1.0f;
    public float rotSpeed = 0.4f;
    public bool hasReproduced;
    public float scalingFactor = 1.1f;
    public float maxScale = 3f;


    public GameObject lightDecider;
    public GameObject glowObj;
    public new DecideLight light;


    private bool MoveToTarget()
    {
        Vector3 lookAtGoal = new Vector3(destination.position.x, transform.position.y, destination.position.z); //we don't want to move our characters y axis shifting them up or down
        Vector3 lookDir = lookAtGoal - transform.position;
        transform.rotation = Quaternion.Slerp(transform.rotation, Quaternion.LookRotation(lookDir), Time.deltaTime * rotSpeed);
        //this.transform.LookAt(lookAtGoal);
        if (Vector3.Distance(transform.position, lookAtGoal) > accuracy) //here we use look at goal so that we don't get jittering
        {
            transform.Translate(0, 0, speed * Time.deltaTime);
            return false;
        }
        return true;
    }
    private bool MoveAwayFromTarget()
    {
        Vector3 lookAtGoal = new Vector3(destination.position.x, transform.position.y, destination.position.z); 
        Vector3 lookDir = transform.position - lookAtGoal;
        transform.rotation = Quaternion.Slerp(transform.rotation, Quaternion.LookRotation(lookDir), Time.deltaTime * rotSpeed);
        //this.transform.LookAt(lookAtGoal);
        if (Vector3.Distance(transform.position, lookAtGoal) < fleeRange) 
        { 
            transform.Translate(0, 0, speed * Time.deltaTime);
            return false;
        }
        return true;
    }

    void Start()
    {
        navPoints = GameObject.FindGameObjectsWithTag("navpoint");
        newPatrolPoint = Instantiate(navPoints[0], GetRandomDest(), Quaternion.identity);
        childrenRend = GetComponentsInChildren<Renderer>();
        currentState = State.Scout;
    }

    void Update()
    {
        if (transform.localScale.x < maxScale)
        {
            Grow();
        }
        CheckTransitions();
        CheckLights();
        Act();
    }
    
    void Grow()
    {
        transform.localScale = new Vector3(transform.localScale.x + transform.localScale.x * scalingFactor * Time.deltaTime, 
            transform.localScale.y + transform.localScale.y * scalingFactor * Time.deltaTime, 
            transform.localScale.z + transform.localScale.z * scalingFactor * Time.deltaTime);
        if (transform.localScale.x >= 2f)
        {
            transform.tag = "enemy";
        }
    }

    void CheckLights()
    {
        if (light.target != null)
        {
            destination = light.target.transform;

            if ((transform.position - light.target.transform.position).magnitude < 1)
            {
                //light.target.GetComponent<Light>().enabled = false;
                Destroy(light);
            }
        }
    }

    void CheckTransitions()
    {
        if (CheckIfInRange("enemy") && hasReproduced)
        {
            if (otherIsLarger())
            {
                currentState = State.Flee;
            }
            else if (!otherIsLarger())
            {
                currentState = State.Chase;
            }
        }
        else if (!CheckIfInRange("enemy"))
        {
            if (currentState == State.Scout)
            {
                if (scoutTimer < 0)
                {
                    currentState = State.Patrol;
                    scoutTimer = 3;
                }

            }
            if (currentState == State.Patrol)
            {
                if (patrolTimer < 0)
                {
                    currentState = State.Reproduce;
                    patrolTimer = 3;
                }
            }
            
        }
        
    }
    void Act()
    {
        switch (currentState)
        {
            case State.Reproduce: 
                {
                    GameObject child = Instantiate(gameObject, transform.position, transform.rotation);
                    child.transform.tag = "child";
                    reproduceTimer = 10;
                }
                break;

            case State.Chase:
                {
                    //enemy in range and targeted
                    lightDecider = GameObject.FindGameObjectWithTag("lightTracker");
                    light = lightDecider.GetComponent<DecideLight>();

                    speed = 3f;
                    ChangeColor(Color.red);
                    bool caught = MoveToTarget();
                    if (caught)
                    {
                        Destroy(destination.gameObject);
                        maxScale += .5f;
                    }
                }
                break;
            case State.Patrol:
                {
                    speed = 1f;
                    ChangeColor(Color.blue);
                    patrolTimer -= Time.deltaTime;
                }
                break;
            case State.Flee:
                {
                    speed = 1.1f;
                    MoveAwayFromTarget();
                    ChangeColor(Color.yellow);

                }
                break;

            case State.Scout:
                {
                    speed = .8f;
                    ChangeColor(Color.green);
                    scoutTimer -= Time.deltaTime;
                }
                break;

            default:
                {
                    break;
                }
        }
    }
    public Transform GetNextNavPoint()
    {
        navPointNum = (navPointNum + 1) % navPoints.Length;
        return navPoints[navPointNum].transform;
    }

    public void AddNewNavPoint()
    {
        navPoints = null;
        GameObject np = Instantiate(newPatrolPoint, gameObject.transform, true);
        np.transform.parent = null;
        navPoints = GameObject.FindGameObjectsWithTag("navpoint");

    }
    public Vector3 GetRandomDest()
    {
        return new Vector3(Random.Range(0, 10), 0f, (Random.Range(0, 10)));
    }

    public void ChangeColor(Color color)
    {
        foreach (Renderer r in childrenRend)
        {
            foreach (Material m in r.materials)
            {
                m.color = color;
            }
        }
    }

    public bool otherIsLarger()
    {
        return Vector3.Magnitude(transform.localScale) < Vector3.Magnitude(destination.localScale);
    }

    //returns true and sets the target to nearest object of that tag for tags like 'navpoint' or 'enemy'
    public bool CheckIfInRange(string tag)
    {
        objectsOfInterest = GameObject.FindGameObjectsWithTag(tag);
        if (objectsOfInterest != null)
        {
            foreach (GameObject g in objectsOfInterest)
            {
                if (Vector3.Distance(g.transform.position, transform.position) < detectionRange)
                {

                    destination = g.transform;
                    return true;
                }
            }
        }
        return false;
    }

}