using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class PlayerMovement : MonoBehaviour {
	public float speed;
	public float speedMult;
	private Vector2 input;
	Rigidbody2D rbody;

    #region Bullet Vars
    public GameObject bulletPref;
    public float cooldown;
    #endregion

    void Start () 
	{
		rbody = GetComponent<Rigidbody2D> ();
	}

	void Update () 
	{
        if (cooldown > 0)
        {
            cooldown -= Time.deltaTime;
        }

        if (Input.GetKey(KeyCode.Mouse0))
        {
            if (cooldown <= 0)
            {
                GameObject.Instantiate(bulletPref, transform.position, bulletPref.transform.rotation);
                cooldown = 0.2f;
            }
        }
		input = new Vector2 (Input.GetAxisRaw ("Horizontal"), Input.GetAxisRaw ("Vertical"));

		if (input != Vector2.zero) 
		{
			rbody.velocity = input * ((speed * speedMult) * Time.deltaTime);
		} 
		else 
		{
			rbody.velocity = Vector2.zero;
		}
	}
}