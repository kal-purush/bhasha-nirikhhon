using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Bug : MonoBehaviour
{
    public float flyspeed = 0.1f;
	public Score s;
	public lookat l;
	
	public GameObject pref;
	public GameObject bloop;
	void Start()
	{
		GameObject go = GameObject.FindWithTag("manager");
		s = go.GetComponent<Score>();
		GameObject gs = GameObject.FindWithTag("looker");
		l = gs.GetComponent<lookat>();
		
		}

    // Update is called once per frame
    void Update()
    {
        transform.Translate(0, 0, flyspeed * 0.1f);
		if(transform.position.z > 50f)
		{
			Destroy(gameObject);
			}
    }
	void OnTriggerEnter(Collider other)
    {
        
		if(other.tag == "dj")
		{
			Debug.Log("bug hut" + other);
			s.Healthdecrease();
			l.lookatcamangri();
			//instantiate fireworks  prefab
			
			GameObject a = Instantiate(pref, transform.position, Quaternion.identity);
			GameObject b = Instantiate(bloop, transform.position, Quaternion.identity);
		Destroy(gameObject);
		}
    }
}