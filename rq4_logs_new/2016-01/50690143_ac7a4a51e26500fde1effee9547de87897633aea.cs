using UnityEngine;
using System.Collections;

public class BearTrap : MonoBehaviour {

    public PhysicsMaterial2D physicsMat;
    public bool isDestroyed = false;

	// Use this for initialization
	void Start ()
    {
        GetComponent<Animator>().enabled = false;
    }
	
	// Update is called once per frame
	void Update () {

    }

    public void destroyRock()
    {
        if(!isDestroyed)
        {
            isDestroyed = true;
            StartCoroutine(MyCoroutine());
        }
    }


    IEnumerator MyCoroutine()
    {
        yield return new WaitForSeconds(0.750f);
        GetComponent<BoxCollider2D>().sharedMaterial = physicsMat;
        GetComponent<Animator>().enabled = false;
        Destroy(GetComponent<Rigidbody2D>());
        Destroy(GetComponent<CircleCollider2D>());
        GetComponent<BoxCollider2D>().size = new Vector3 (GetComponent<BoxCollider2D>().size.x * 1.3f, GetComponent<BoxCollider2D>().size.y);
        yield return new WaitForSeconds(8.0f);
        Destroy(gameObject);
    }


}