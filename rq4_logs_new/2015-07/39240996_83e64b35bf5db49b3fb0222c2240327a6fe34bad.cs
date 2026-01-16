using UnityEngine;
using System.Collections;

public class EnemyController : MonoBehaviour 
{
    public Transform target;
    public int moveSpeed;
    public int rotationSpeed;
    public int maxdistance;
    private Transform myTransform;
    public PlayerController playerController;
    public bool inRange = false;
    public float timer = 3;

	// Use this for initialization
	void Start () 
    {
	    myTransform = transform;
	}
	
	// Update is called once per frame
    void Update()
    {
        timer -= Time.deltaTime;

       
     
        if (Vector3.Distance(target.position, myTransform.position) < maxdistance)
        {
            //Move towards target
            transform.LookAt(target.position);
            myTransform.position += myTransform.forward * moveSpeed * Time.deltaTime;
            inRange = true;
        }
        else
            inRange = false;
    }
    void OnTriggerStay(Collider other)
    {
        if (other.tag == "Player" && inRange == true && timer <= 0) //&& timer dosne't work here for some reason
        {
            timer = 3;
            playerAttack();//only damages per collision - maybe there is a loop needed here?
        }
        
    }
    void playerAttack()
    {
        playerController.playersHealth -= 10;
    }
    
}