using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class GameManager : MonoBehaviour {
    Camera mainCamera;
    GameObject clickedObject;
    GameObject spawnerArea;
    GameObject[] doors;

    public int doorsPassed { get; set; }
    public int combo { get; set; }

    int currentLevel = 0;

    // Use this for initialization
    void Start () {
        mainCamera = Camera.main;
        mainCamera.enabled = true;

        spawnerArea = GameObject.Find("SpawnerArea");
        // Debug.Log("SpawnerArea: " + spawnerArea + " Location: " + spawnerArea.transform.position);
    }
	
	// Update is called once per frame
	void Update () {
        doors = GameObject.FindGameObjectsWithTag("SlidingDoor");

        if (Input.GetMouseButtonDown(0))
        { // if left button pressed...
            Ray ray = mainCamera.ScreenPointToRay(Input.mousePosition);
            RaycastHit hit;
            if (Physics.Raycast(ray, out hit))
            {
                // the object identified by hit.transform was clicked
                clickedObject = hit.transform.gameObject;
                clickedObject.GetComponent<SlidingDoor>().operateDoor();
                //Debug.Log("Clicked: " + clickedObject.name + " open: " + clickedObject.GetComponent<SlidingDoor>().isOpen);

                foreach (GameObject door in doors)
                {
                    if(clickedObject.name != door.name)
                    {
                        door.GetComponent<SlidingDoor>().closeDoor();
                    }
                }
            } 
        }
    }

    public void clearCombo()
    {
        combo = 0;
    }

    public void addCombo()
    {
        combo++;
    }

    public void addPass()
    {
        doorsPassed++;

        if (doorsPassed % 10 == 0)
        {
            updateLevel();
        }
    }

    void updateLevel()
    {
        currentLevel++;
    }
}