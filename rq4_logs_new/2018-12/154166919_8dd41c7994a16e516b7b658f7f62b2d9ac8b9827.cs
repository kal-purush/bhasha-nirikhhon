using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Window_flash : MonoBehaviour {
    public GameObject window;

    private void OnTriggerEnter2D(Collider2D collision)
    {
        if (collision.gameObject.name == "car")
        {
            window.SetActive(true);
            Invoke("Window_SetInactive", 0.5f);
        }
    }

    void Window_SetInactive()
    {
        window.SetActive(false);
    }

    // Use this for initialization
    void Start () {

	}
	
	// Update is called once per frame
	void Update () {
		
	}
}