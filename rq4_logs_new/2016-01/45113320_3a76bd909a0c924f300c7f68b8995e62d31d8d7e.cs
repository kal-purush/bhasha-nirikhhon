using UnityEngine;
using System.Collections;

public class Fire : MonoBehaviour {
    public float aliveTime =2f;
    float timeFromStart;
    public float speed;
    public Vector3 direction;
    public Transform trs;
    public bool isReady;
	// Use this for initialization
	void Start () {
	}
	
	// Update is called once per frame
	void Update () {
        if (isReady)
        {
            
            timeFromStart += Time.deltaTime;
            if (timeFromStart < aliveTime)
            {
                Debug.Log("Move");
                Move();
            }
            else
            {
                Destroy(this.gameObject);
            }
        }
	}

    public void Init( Vector3 dir, float tAlive = 0)
    {
        if(tAlive!=0)
            aliveTime = tAlive;
        timeFromStart = 0;
        direction = dir;
        dir.Normalize();
        trs = this.GetComponent<Transform>();
        isReady = true;
     }

    void Move()
    {
        Vector3 pos = trs.position;
        this.transform.position = new Vector3(pos.x+direction.x * speed*Time.deltaTime , pos.y + direction.y * speed * Time.deltaTime, pos.z );
    }
}