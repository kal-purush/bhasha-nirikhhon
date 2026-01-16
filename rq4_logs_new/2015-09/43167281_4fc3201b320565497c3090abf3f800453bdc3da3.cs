using UnityEngine;
using System.Collections;

public class CharacterShoot : MonoBehaviour {

    public float coolDown = 0.5f;
    public float projectileSpeed = 1f;
    private float timer = 0f;

    public Transform shootSpot;
    public GameObject projectile;

	// Use this for initialization
	void Start () {

	}
	
	// Update is called once per frame
	void Update () {
        CheckForShoot();
	}

    void CheckForShoot() {
        if (Input.GetKeyDown(KeyCode.Space)) {
            if(timer <= 0) {
                GameObject proj;
                proj = (GameObject)Instantiate(projectile, shootSpot.position, shootSpot.rotation);
                proj.GetComponent<Rigidbody2D>().velocity = new Vector2(projectileSpeed * this.GetComponent<Transform>().localScale.x, 0f);
                //Debug.Log(this.transform.localScale.x.ToString);
                timer = coolDown;
            }
        }
        timer -= Time.deltaTime;
    }
}