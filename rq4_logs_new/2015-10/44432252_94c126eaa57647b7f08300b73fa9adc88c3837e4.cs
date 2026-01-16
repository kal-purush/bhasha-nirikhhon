using UnityEngine;
using System.Collections;

public class CreeperController : MonoBehaviour {
	
	public double health;
	public Vector3 offset;
	public float speed;
	public float speedRandFactor;
	
	private Rigidbody2D rb2d;
	private Vector2 mov;
	private GameObject target;
	private Vector3 targetPos;
	
	// Use this for initialization
	void Start () {
		target = GameObject.FindGameObjectWithTag("Player");
		targetPos = transform.position;
		rb2d = GetComponent<Rigidbody2D>();
	}
	
	// Update is called once per frame
	void Update () {
		if (health <= 0)
		{
			Destroy(this.gameObject);
		}
		
		if (target)
		{
			var effectiveSpeed = speed + Random.Range(-speedRandFactor, speedRandFactor);
			Vector3 posNoZ = transform.position;
			posNoZ.z = target.transform.position.z;
			
			Vector3 movement = (target.transform.position - posNoZ).normalized * effectiveSpeed;
			mov = new Vector3(movement.x, movement.y);
			//targetPos = transform.position + (targetDirection.normalized * velocity * Time.deltaTime);
			//transform.position = Vector3.Lerp(transform.position, targetPos + offset, 0.25f);
			//transform.position = transform.position + (targetDirection.normalized * velocity * Time.deltaTime);
		}
		else
		{
			mov = new Vector3(0, 0, 0);
		}
	}
	
	void FixedUpdate()
	{
		rb2d.AddForce(mov);
	}
	
}