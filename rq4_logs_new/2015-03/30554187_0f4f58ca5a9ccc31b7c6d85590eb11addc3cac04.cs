using UnityEngine;
using System.Collections;

public class Cannon : MonoBehaviour {

	public enum Cannontype
	{
		type1//TODO: list cannon types here
	};

	public Cannontype cannontype;

	//health that cannon subtracts from the ship it hits
	public int attackPower;
	public int shootSpeed;
	public float shootRange;

	public Vector3 origin;


	void Start () {
	
	}
	
	// Update is called once per frame
	void Update () {
		//TODO: destroy after passing shooting range from origin
	}

}