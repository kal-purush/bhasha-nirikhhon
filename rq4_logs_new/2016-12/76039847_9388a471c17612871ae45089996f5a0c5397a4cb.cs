using UnityEngine;
using System.Collections;

public class Ballon : MonoBehaviour 
{
	public Player player = null;
	public Transform ballonGroup = null;

	private int number = 0;

	void Start () 
	{
		number = player.Life;

		BallonUpdate ();
	}

	void BallonUpdate()
	{
		for (int i = 0; i < ballonGroup.childCount; i++)
			ballonGroup.GetChild (i).gameObject.SetActive (false);

		for (int i = 0; i <number; i++)
			ballonGroup.GetChild (i).gameObject.SetActive (true);
	}

	void Update ()
	{
		if (!number.Equals (player.Life)) 
		{
			number = player.Life;
			BallonUpdate ();
		}
	}
}