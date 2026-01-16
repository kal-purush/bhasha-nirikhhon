using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ChargeShieldBehavior : MonoBehaviour {
	Rigidbody2D rig;
	GameObject player;
    public GameObject hurtsEnemy;
	public int health = 10;
	public float relativePos;
	private float distX;
	private bool lookingLeft;
    Animator animator;

	[Range(0, 1000)]
	public int attackProb = 300;

	[Range(-1,3)]
	public int state = -1;
	/**
     * States:
     * -1 = awakening
     *  0 = Idle
     *  1 = Attack
     *  2 = Attacking
     *  3 = Dead
     */

	// Use this for initialization
	void Start () {
		player = GameObject.FindGameObjectWithTag("Player");
		rig = GetComponent<Rigidbody2D>();
		lookingLeft = false;
        animator = GetComponent<Animator>();
	}
	
	// Update is called once per frame
	void Update () {
		distX = player.transform.position.x - transform.position.x;
		if (health <= 0) {
			state = 3;
            animator.SetBool("Death", true);
		}
			
		if (state == 0) {
			if (lookingLeft && distX > 0) {

				lookingLeft = false;
                transform.localScale = new Vector2(Mathf.Abs(transform.localScale.x), Mathf.Abs(transform.localScale.y)) ;
			} else if (!lookingLeft && distX < 0) {
				lookingLeft = true;
				transform.localScale = new Vector2(-Mathf.Abs(transform.localScale.x), Mathf.Abs(transform.localScale.y));
            }
			if (Mathf.Abs(distX) < 40f) {
				//rig.velocity = new Vector2(Mathf.Sign(distX) * -5, 0);
			}
		}
		if (state == -1) {
			state = 0;
		} else if (state == 1) {
			//GetComponent<SwordBehaviour> ().activated = true;
			Attack ();
		}
		if (Random.Range(0, attackProb) == 1 && !cooloff && !animator.GetBool("Awakening") && !animator.GetBool("Hurt") && !animator.GetBool("Death")) {
			state = 1;
			//rig.constraints = RigidbodyConstraints2D.FreezePositionY;
		}

        //Animation Triggers
        animator.SetBool("Running", state == 1);

	}
	float timeout = 0;
	void Attack () {
		if (Mathf.Abs(distX) > 2) {
			rig.velocity = new Vector2(Mathf.Sign(distX) * 20, 0);
            if (rig.velocity.x > 0) {
                transform.localScale = new Vector2(Mathf.Abs(transform.localScale.x), Mathf.Abs(transform.localScale.y));
            } else {
                transform.localScale = new Vector2(-Mathf.Abs(transform.localScale.x), Mathf.Abs(transform.localScale.y));
            }
		} else {
			state = 2;
            if (state != 2) {
                StartCoroutine(attack());
            }
			return;
		}
		if (timeout > 2 && distX > 5) {
			state = 0;
			timeout = 0;
            StartCoroutine(cooldown());
		}
		timeout += Time.deltaTime;
	}
	IEnumerator attack() {
		yield return new WaitForSeconds(3);
		state = 0;
	}

    bool cooloff = false;
    IEnumerator cooldown() {
        if (cooloff == false) {
            cooloff = true;
            yield return new WaitForSeconds(1.0f);
            cooloff = false;
        }
    }
	public void hurt() {
		health -= 10;
		state = 0;
	}

	public void Death() {
		Destroy (this.gameObject);
	}

    void OnTriggerEnter2D(Collider2D other) {
        if (other.gameObject.tag == "Player") {
            if (state == 1) {
                state = 0;
                rig.velocity = new Vector2(-Mathf.Sign(transform.localScale.x) * 18.0f, 0);
            }
        }
    }

}