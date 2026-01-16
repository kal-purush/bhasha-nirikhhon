using UnityEngine;
using System.Collections;

public class ShipController : MonoBehaviour {

	private Rigidbody2D rb; //Player rigid body

	public GameObject shot;
	public ShotScript shotScript;

	public float health = 50f;
	public float minSpeed = 13f;
	public float medSpeed = 25f;
	public float maxSpeed = 50f;
	public float rotationalSpeed = 50f;

	private float currentSpeed;
	private bool turning = false;
	private bool shotSpawnToggle = false;
	private Vector3 turnAngle = new Vector3(0,0,0);

	public Transform shotSpawn1, shotSpawn2;

	void Start () {
		rb = GetComponent<Rigidbody2D> ();
		currentSpeed = minSpeed;
	}

	void Update(){ 

		if(Input.GetKeyDown("up")){
			turnByDegrees(90f);
		}

		if(Input.GetKeyDown("down")){ //anything bigger than 180 it takes the long way around. Get Dom to fix
			turnByDegrees(270f);
		}

		if(Input.GetKeyDown("left")){
			turnByDegrees(180f);
		}

		if(Input.GetKeyDown("right")){
			turnByDegrees(0f);
		}
		
		if (Input.GetKeyDown("space")){ 
			fireShot(90f); 
		}

		if (health <= 0){
			deathSequence();
		}
	}

	void FixedUpdate () { //fixed update is for physics calculations
		rb.AddForce(transform.right * currentSpeed * Time.deltaTime, ForceMode2D.Force);

		//speed limit
		if(rb.velocity.magnitude > currentSpeed) 
        {
         	rb.velocity = rb.velocity.normalized * maxSpeed;
        }

        //turning
		if (turning)
	     {
			if (Vector3.Distance(transform.eulerAngles, turnAngle) > 0.1f)
	         {
				transform.eulerAngles = Vector3.Lerp(transform.rotation.eulerAngles, turnAngle, Time.deltaTime);
	         }
	         else
	         {
				transform.eulerAngles = turnAngle;
				turning = false;
				turnAngle.Set(0,0,0);
				Debug.Log("turning reset");
	         }
	     }

	}

	void OnTriggerEnter2D(Collider2D other){
		if(other.gameObject.tag == "Shot"){
			health = health - shotScript.damage;
		}
	}

	void changeSpeed(string speed){
		if (speed.Equals("minSpeed"))
			currentSpeed = minSpeed;
		else if(speed.Equals("medSpeed"))
			currentSpeed = medSpeed;
		else if(speed.Equals("maxSpeed"))
			currentSpeed = maxSpeed;
	}

	void turnByDegrees(float angle){ //will turn to that relative to world space
		turning = true;
		turnAngle.Set(0,0, angle);
	}

	void fireShot(float angle){ //fires Shot prefab at that angle relative to world space
		Quaternion rotation = Quaternion.Euler(0, 0, angle - 90);

		if(shotSpawnToggle){
			Instantiate(shot, shotSpawn1.position, rotation);
			shotSpawnToggle = false;
		}
		else{
			Instantiate(shot, shotSpawn2.position, rotation);
			shotSpawnToggle = true;
		}
		
	}

	void deathSequence(){
		//will late impliment: instanciate explosion animation and destroyed ship prefab
		Destroy(this.gameObject);
	}
}