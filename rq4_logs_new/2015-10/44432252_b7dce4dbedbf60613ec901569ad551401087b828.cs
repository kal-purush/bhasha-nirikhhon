using UnityEngine;
using System.Collections;
using System.Timers;

public class ZombieGen : MonoBehaviour {
	public float minWidthOffset;
	public float maxWidthOffset;
	
	public float timeoutInSeconds;
	public float randomizeTimeout;
	public float probZ1;
	public GameObject Z1;
	public float probZ2;
	public GameObject Z2;
	public float probZ3;
	public GameObject Z3;
	private float instantiationT;
	// Use this for initialization
	void Start ()
	{
		instantiationT = timeoutInSeconds + Random.Range(-randomizeTimeout, randomizeTimeout);
	}
	
	void SpawnZombie()
	{
		
		float r = Random.Range(0, probZ1 + probZ2 + probZ3);
		
		GameObject type;
		if (r <= probZ1)
		{
			type = Z1;
		}
		else if (r <= probZ2 + probZ1)
		{
			type = Z2;
		}
		else
		{
			type = Z3;
		}
		
		var playerControl = GameObject.FindGameObjectWithTag("Player").GetComponent<PlayerMovement>();
		
		// Pick a random position for as long as there's a clash
		float x, y;
		int i = 0;
		do
		{
			
			y = Random.Range(playerControl.yLimitMin, playerControl.yLimitMax);
			x = Random.Range(this.minWidthOffset, this.maxWidthOffset);
			if (Random.value < 0.5)
			{
				x = -x;
			}
			x += playerControl.transform.position.x;
			i++;
		} while (this.InvalidSpawnPoint(x, y) && i < 200);
		
		if (i < 200)
			Instantiate(type, new Vector3(x, y, 0), Quaternion.identity);
	}
	
	private bool InvalidSpawnPoint(float x, float y)
	{
		float maxX = GameObject.FindGameObjectWithTag("Player").GetComponent<PlayerMovement>().xLimitMax;
		float maxY = GameObject.FindGameObjectWithTag("Player").GetComponent<PlayerMovement>().yLimitMax;
		float minX = GameObject.FindGameObjectWithTag("Player").GetComponent<PlayerMovement>().xLimitMin;
		float minY = GameObject.FindGameObjectWithTag("Player").GetComponent<PlayerMovement>().yLimitMin;
		if (x < minX || y < minY) return true;
		if (x > maxX|| y > maxY) return true;
		if (Physics.OverlapSphere(new Vector3(x, y, 0), 1).Length > 0) return true;
		
		return false;
	}
	// Update is called once per frame
	void Update ()
	{
		instantiationT -= Time.deltaTime;
		
		if (instantiationT <= 0)
		{
			SpawnZombie();
			instantiationT = timeoutInSeconds + Random.Range(-randomizeTimeout, randomizeTimeout);
		}
	}
}