using UnityEngine;
using System.Collections;

public class wallSound : MonoBehaviour {

	public AudioSource hit; 
	
	void OnCollisionEnter2D(Collision2D other)
	{
		if(other.gameObject.tag == "Player")
		{
			hit.Play();
		}
	}

}