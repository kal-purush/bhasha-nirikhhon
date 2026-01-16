using UnityEngine;
using System.Collections;

[RequireComponent(typeof(NavMeshAgent))]
[RequireComponent(typeof(RigidbodyController))]
[RequireComponent(typeof(Animator))]
[RequireComponent(typeof(AudioSource))]
public class AIController : MonoBehaviour {

    public UserController player;
    public float turning;
    public float distThreshold;
    public float speed;

    private NavMeshAgent agent;
    private Animator anim;
    private RigidbodyController character;
    private Transform trans;
    private AudioSource aud;

    public bool travelling;
    private Vector3 targetPosition;
    private Vector3 STOP_VECTOR = new Vector3(-10000, -10000, -10000);
    

	// Use this for initialization
	void Awake () {

        character = GetComponent<RigidbodyController>();
        trans = GetComponent<Transform>();
        anim = GetComponent<Animator>();
        agent = GetComponent<NavMeshAgent>();
        aud = GetComponent<AudioSource>();

        agent.updatePosition = false;

        stopMoving();
        
    }
	
	// Update is called once per frame
	void Update () {

        
        moveTowards(targetPosition);
	}

    void moveTowards(Vector3 target)
    {

        agent.destination = target;

        if (target.Equals(STOP_VECTOR) || agent.desiredVelocity.Equals(Vector3.zero))
        {
            travelling = false;
            return;
        }
        

        Vector2 right = new Vector2(trans.right.x, trans.right.z);
        Vector2 desired = new Vector2(agent.desiredVelocity.normalized.x, agent.desiredVelocity.normalized.z);

        float angle = Vector2.Angle(agent.desiredVelocity.normalized, trans.forward);
        if (Vector2.Dot(right, desired) < 0)
        {
            angle = -angle;
        }

        character.setTurn(turning * angle);

        if ((agent.destination - trans.position).magnitude < distThreshold)
        {
            character.setSpeed(0);
            agent.updateRotation = false;
            travelling = false;

        } else {
            character.setSpeed(speed);
            agent.updateRotation = true;
            travelling = true;
        }

    }

    void OnAnimatorMove()
    {

        this.transform.position = anim.rootPosition;
        this.transform.rotation = anim.rootRotation;

        agent.nextPosition = trans.position;

    }

    public bool isTravelling()
    {
        return travelling;
    }

    public void moveTo(Vector3 target)
    {
        targetPosition = target;   
    }

    public void stopMoving()
    {
        travelling = false;
        targetPosition = STOP_VECTOR;
    }

    public NavMeshAgent getAgent()
    {
        return agent;
    }

    public Transform getTransform()
    {
        return trans;
    }

    public RigidbodyController getController()
    {
        return character;
    }

    public Animator getAnimator()
    {
        return anim;
    }

    public AudioSource getAudioSource()
    {
        return aud;
    }
}