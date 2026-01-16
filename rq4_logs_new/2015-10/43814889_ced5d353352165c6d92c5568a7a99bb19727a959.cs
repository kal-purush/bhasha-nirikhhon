using UnityEngine;
using System.Collections;

public class ball : MonoBehaviour {
    public float launchSpeed;
	private AudioSource audioSource;
    private Rigidbody rigidBody;
	// Use this for initialization
	void Start () {
        rigidBody = GetComponent<Rigidbody>();
		audioSource = GetComponent<AudioSource>();

        Launch ();

	}

	void Launch ()
	{
		rigidBody.velocity = new Vector3 (0, 0, launchSpeed);
		audioSource.Play ();
	}
	
	// Update is called once per frame
	void Update () {
	
	}
}