using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class MessagesController : MonoBehaviour {

    public string message;
    public TextMesh popUp;

    // Use this for initialization
    void Start () {

	}
	
	// Update is called once per frame
	void Update () {
		
	}

    private void OnTriggerEnter2D(Collider2D collision)
    {
        if (collision.CompareTag("Player"))
        {
            showPopUp(message);

            gameObject.SetActive(false);
        }
    }

    void showPopUp(string message)
    {
        popUp.text = message;
        Instantiate(popUp, transform.position, transform.rotation);
    }
}