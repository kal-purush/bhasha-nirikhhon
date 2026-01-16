using UnityEngine;
using System.Collections;

public class PlayZombie : MonoBehaviour {

    public AudioClip zombieDeath;
	void Start () {
        this.gameObject.GetComponent<AudioSource>().PlayOneShot(zombieDeath);
	}
	
	// Update is called once per frame
	void Update () {
	
	}
}