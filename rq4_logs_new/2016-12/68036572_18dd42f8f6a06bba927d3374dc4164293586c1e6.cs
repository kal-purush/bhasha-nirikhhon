using UnityEngine;
using System.Collections;
using UnityEngine.EventSystems;

public class SelectOnInput : MonoBehaviour {

    public EventSystem eventSystem;
    public GameObject selectedObject;

    private bool isButtonSelected;

	// Use this for initialization
	void Start () {
	
	}
	
	// Update is called once per frame
	void Update () {
	    if(Input.GetAxisRaw("Vertical") != 0 && isButtonSelected == false)
        {
            eventSystem.SetSelectedGameObject(selectedObject);
            isButtonSelected = true;
        }
	}

    private void OnDisable()
    {
        isButtonSelected = false;
    }
}