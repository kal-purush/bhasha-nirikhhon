using UnityEngine;

public class BrickController : MonoBehaviour
{
    public int maxHits;
    public int timesHit;
	// Use this for initialization
	void Start ()
    {
        timesHit = 0;
	}

    private void OnCollisionEnter2D(Collision2D collision)
    {
        print("Brick Hit!");
        if(++timesHit >= maxHits)
        {
            Destroy(this.gameObject);
        }
    }

    // Update is called once per frame
    void Update ()
    {
		
	}
}