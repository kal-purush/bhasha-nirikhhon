using UnityEngine;
using System.Collections;

public class MeteoScript : MonoBehaviour {

	GameObject cloud1;
	GameObject cloud2;

	GameObject cloud;
	GameObject papaCloud;

	public float delay;
	float timerCloud;

	int nbCloud;

	// Use this for initialization
	void Start () {
		papaCloud = new GameObject ();
		papaCloud.name = "PapaCloud";
		cloud1 = Resources.Load ("Prefab/Cloud") as GameObject;
		cloud2 = Resources.Load ("Prefab/CloudBis") as GameObject;
		timerCloud = delay;
		nbCloud = 0;
	}
	
	// Update is called once per frame
	void Update () 
	{
		timerCloud += Time.deltaTime;

		if (timerCloud >= delay)
		{
			int rnd = Random.Range(0,2);
			if (rnd == 0){cloud = Instantiate(cloud1);}
			else{cloud = Instantiate(cloud2);}
			cloud.transform.parent = papaCloud.transform;
			cloud.name = "Cloud-" + nbCloud;
			nbCloud++;
			timerCloud = 0;
		}
	}
}