using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.XR.WSA;

public class AudioManager : MonoBehaviour
{

	//audio sources for the specific parts of the track
	
	public GameObject audSource;
	
	public AudioSource audio;
	
	public float volume = 0f;

	public Vector3 distanceVector;

	public BoxCollider2D collider;

	public GameObject midpoint;

	private bool enteredBox = false;
	void OnCollisionEnter2D (Collision2D collision)
	{
		if (collision.gameObject.tag == "Fuck")
		{
			enteredBox = true;
			play(volume);
			volume = 0.5f;
			volume--;
		}
		
		//volume = 0f;
	}

	private void OnCollisionStay2D(Collision2D collision)
	{
		if (collision.gameObject.tag == "Fuck")
		{
			if (enteredBox)
			{
				float distance = distanceVector.magnitude;
				audio.volume = 1 / distance;
				Debug.Log("Fuck me");
			}
		}
	}

	void OnCollisionExit2D(Collision2D collision)
	{
		//if (collision.gameObject.tag == "Fuck")
		//{
			volume = 0f;
		//}
	}

	void play(float volume)
	{
		audio.volume = volume;
		audio.Play();
	}

	void FixedUpdate()
	{
		audio.volume = volume;
		distanceVector = midpoint.GetComponent<Transform>().position - transform.position;
		Debug.Log(volume);
	}


}