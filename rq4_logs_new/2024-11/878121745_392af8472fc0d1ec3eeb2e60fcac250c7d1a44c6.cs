using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Door : MonoBehaviour
{
    public GameObject keypad;
    public GameObject door;
    public GameObject exitGlass;
    public bool openDoor = false;
    // Start is called before the first frame update
    void Start()
    {
        keypad.SetActive(false);
        exitGlass.SetActive(false);
    }

    // Update is called once per frame
    void Update()
    {
        
    }

   private void OnTriggerEnter(Collider other) {
    if(other.CompareTag("Player")){
        keypad.SetActive(true);
        Debug.Log($"Player in Door Zone");
    }
   }//end onTriggerEnter

    private void OnTriggerExit(Collider other) {
        if(other.CompareTag("Player")){
            keypad.SetActive(false);
            Debug.Log($"Player has left the Door Zone");
        }
    }//end onTriggerExit

    public void OpenDoor(){
        openDoor = true;
        exitGlass.SetActive(true);
        door.SetActive(false);
    }//end open door


}//endDoorClass