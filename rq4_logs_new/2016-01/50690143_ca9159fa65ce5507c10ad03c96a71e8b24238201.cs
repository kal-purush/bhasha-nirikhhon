using UnityEngine;
using System.Collections;

public class Rock : MonoBehaviour {

    public  GameObject cloudChild;
	// Use this for initialization
	void Start ()
    {
        GetComponent<Animator>().enabled = false;
        GameObject cloud = (GameObject)Instantiate(cloudChild, this.transform.position + new Vector3(0.0f, 15.0f, 0.0f), new Quaternion());
        cloud.transform.parent = this.transform;
        cloud.tag = "Obstacles";
        cloud.GetComponent<Animator>().enabled = false;
        cloudChild = cloud;
    }
	
	// Update is called once per frame
	void Update () {

        if (Input.GetKeyDown(KeyCode.R))
        {
            GetComponent<Animator>().enabled = true;
            StartCoroutine(MyCoroutine());
        }
        if (Input.GetKeyDown(KeyCode.C))
        {
            cloudChild.GetComponent<Animator>().enabled = true;
            StartCoroutine(MyCloudCoroutine());
        }

    }


    IEnumerator MyCoroutine()
    {
        yield return new WaitForSeconds(0.583f);
        GetComponent<Animator>().enabled = false;
        Destroy(GetComponent<Rigidbody2D>());
        Destroy(GetComponent<CircleCollider2D>());
        yield return new WaitForSeconds(8.0f);
        Destroy(gameObject);
    }
    IEnumerator MyCloudCoroutine()
    {
        yield return new WaitForSeconds(0.333f);
        cloudChild.GetComponent<Animator>().enabled = false;
        GetComponent<Animator>().enabled = true;
        Destroy(cloudChild.gameObject);
        StartCoroutine(MyCoroutine());
    }
}