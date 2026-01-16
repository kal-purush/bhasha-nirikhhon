using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Trash : MonoBehaviour
{
	public delegate void CompletedFriedFoodDelegate(FriedFood friedFood);
	public CompletedFriedFoodDelegate completedFriedFoodDelegate;

	// Start is called before the first frame update
	void Start()
	{

	}

	// Update is called once per frame
	void Update()
	{

	}


	private void OnTriggerEnter2D(Collider2D collision)
	{
		if (collision.gameObject.tag == "food")
		{
			completedFriedFoodDelegate(null);
			Destroy(collision.gameObject);
		}
	}
}