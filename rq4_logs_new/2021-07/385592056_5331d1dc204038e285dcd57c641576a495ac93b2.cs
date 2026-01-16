using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.SceneManagement;
public class DoorTrigger : MonoBehaviour
{

    public GameObject button;
    public ButtonTrigger buttonCode;
    public bool isOverlapping = false;

    public void OnTriggerEnter2D (Collider2D other) {

        if (other.gameObject.tag == "Player") {

        isOverlapping = true;

        }
        
        
    }

    public void OnTriggerExit2D (Collider2D other) {

    if (other.gameObject.tag == "Player") {

        isOverlapping = false;

    }

    }

    // Update is called once per frame
    void Update()
    {

        if (isOverlapping && buttonCode.hasbeenPressed) {
            
            if(Input.GetButtonDown("Enter")) {
                SceneManager.LoadScene(0);
            }

        }
    }
}