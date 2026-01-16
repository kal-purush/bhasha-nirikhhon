
using UnityEngine;
using System.Collections;

public class Ninja : MonoBehaviour {


	// Variables
	protected int points = 0;
	protected int speed = 8;
	protected int ninjaHealth = 650;
	protected Animator animator;
	protected Rigidbody2D myRig;
	protected GameObject playerObj;
	public float xMovement;
	public float yMovement;
	private bool isfroze;
		

	//Controls
	public void Controls()
	{

		//Player Controls--------------------------------------->
		xMovement = Input.GetAxis("Horizontal");
		yMovement = Input.GetAxis("Vertical");

		myRig.velocity = new Vector2(0, 0);


		//Move up

		if(yMovement > 0.3)
		{

			myRig.velocity = new Vector2(myRig.velocity.x, speed);
		}

		//Move down						

		if(yMovement < -0.3)
		{

			myRig.velocity = new Vector2(myRig.velocity.x, -speed);
		}

		//Move left

		if(xMovement  < -0.3)
		{	
			myRig.velocity = new Vector2(-speed, myRig.velocity.y);			
		}

		//Move right

		if(xMovement > 0.3)
		{
			myRig.velocity = new Vector2(speed, myRig.velocity.y);
		}


		//Stop player movement
		if (isfroze) 
		{
			freezePlayer ();
		}


		//-----------------------------------------------End of Controls


	}

	//Handing player freeze------------------------------------->>


	public static bool frozen(bool fr)
	{
		isfroze = fr;
		return isfroze;
	}

	public void freezePlayer()
	{
		isfroze = true;
		speed = 0;
	}



	//Handling Player Damage Start------------------------------------->>
	public static float currentHealth()
	{
		//This will be sent to the Global script every frame
		return ninjaHealth;
	}

	public static float addDamage(int dmgPoints)
	{
		ninjaHealth -= dmgPoints;
		return ninjaHealth;
	}


	//Handling player hit detection------------------------------>>

	void OnCollisionEnter2D(Collision2D coll) {
		
		playerObj.SendMessage("addDamage", 100);

	}
		



}