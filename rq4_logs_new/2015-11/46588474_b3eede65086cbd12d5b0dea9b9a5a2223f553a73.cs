using UnityEngine;
using System.Collections;

public class MovementMonster : MonoBehaviour {

    [SerializeField]
    protected float distance;
    [SerializeField]
    protected float speed;

    protected float startX;

    // -1 ou 1
    protected int forward = 1;

    public void setStartX(float x)
    {
        startX = x;
    }

	// Use this for initialization
	void Start () {
        setStartX(gameObject.transform.position.x); // TEMP  
	}
	
	// Update is called once per frame
	void Update () {
        gameObject.transform.Translate(new Vector2(forward * speed / 100, 0));
        if (gameObject.transform.position.x > startX + distance || gameObject.transform.position.x < startX - distance)
        {
            forward = -forward;
        }
    }

    void OnTriggerEnter2D(Collider2D coll)
    {
        // evaluation paresseuse
        if (coll.tag == "Wall" && !coll.gameObject.GetComponent<Placement>().IsMoving())
        {
            forward = -forward;
        }
   }
}