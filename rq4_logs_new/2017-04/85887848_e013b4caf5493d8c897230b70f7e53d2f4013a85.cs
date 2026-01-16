using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class torreta : MonoBehaviour {

    public GameObject player;
    public GameObject explosion;
    public GameObject bullet;
    public Transform spawnPos;


    // Use this for initialization
    void Start() {
        StartCoroutine(shoot());
	}
	
	// Update is called once per frame
	void Update () {
        transform.LookAt(player.transform);
	}

    private void OnTriggerEnter(Collider other)
    {
        if(other.tag == "Bullet")
        {
            Instantiate(explosion, transform.position, Quaternion.identity);
            Destroy(gameObject);
        }
    }

    IEnumerator shoot()
    {
        while (true)
        {
            yield return new WaitForSeconds(1f);
            Instantiate(bullet, spawnPos.position, spawnPos.rotation);
        }
        
    }
}