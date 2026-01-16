using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.AI;

public class PacMacScript : MonoBehaviour
{
    Rigidbody rb;
    NavMeshAgent pinkGhostAgent;
    TextMesh theScoreTextMesh;

    public GameObject scoreText;
    public float speed = 20.0f;
    public GameObject pinkGhost;

    private bool goForward = false;
    private bool goBackward = false;
    private bool goRight = false;
    private bool goLeft = false;

    void Awake()
    {
        rb = this.gameObject.GetComponent<Rigidbody>();
        pinkGhostAgent = this.pinkGhost.GetComponent<NavMeshAgent>();
        pinkGhostAgent.speed = 2.0f;
        this.theScoreTextMesh = this.scoreText.GetComponent<TextMesh>();
    }

    // Start is called before the first frame update
    void Start()
    {
        this.theScoreTextMesh.text = "WOOT!!!";
    }

    // Update is called once per frame
    
    void Update()
    {
        this.pinkGhostAgent.SetDestination(this.gameObject.transform.position);

        if (goForward)
        {
            rb.velocity = Vector3.forward * speed;
        }
        else if(goBackward)
        {
            rb.velocity = Vector3.back * speed;
        }
        else if(goLeft)
        {
            rb.velocity = Vector3.left * speed;
        }
        else if(goRight)
        {
            rb.velocity = Vector3.right * speed;
        }

        if (Input.GetKeyDown("up"))
        {
            this.transform.rotation = Quaternion.LookRotation(Camera.main.transform.up);
            goForward = true;
            goBackward = false;
            goRight = false;
            goLeft = false;
            
        }
        else if (Input.GetKeyDown("down"))
        {
            this.transform.rotation = Quaternion.LookRotation(-Camera.main.transform.up);

            goForward = false;
            goBackward = true;
            goRight = false;
            goLeft = false;
            
        }
        else if (Input.GetKeyDown("left"))
        {
            this.transform.rotation = Quaternion.LookRotation(-Camera.main.transform.right);

            goForward = false;
            goBackward = false;
            goRight = false;
            goLeft = true;
            
        }
        else if (Input.GetKeyDown("right"))
        {
            this.transform.rotation = Quaternion.LookRotation(Camera.main.transform.right);

            goForward = false;
            goBackward = false;
            goRight = true;
            goLeft = false;
            
        }
    }
}