using UnityEngine;
using UnityEngine.UI;
using UnityEngine.EventSystems;
using System.Collections;
using System.Collections.Generic;

public class Pointer : MonoBehaviour {

	RectTransform rt;
    List<AnalogueButtons> selectedButtons = new List<AnalogueButtons>();

	// Use this for initialization
	void Start () {
		rt = GetComponent<RectTransform> ();
	}
	
	// Update is called once per frame
	void Update () {
		rt.Translate (Input.GetAxis ("Horizontal"), Input.GetAxis ("Vertical"), 0);

        PointerEventData pointer = new PointerEventData(EventSystem.current);
        // convert to a 2D position
        pointer.position = Camera.main.WorldToScreenPoint(transform.position);
        var raycastResults = new List<RaycastResult>();
        EventSystem.current.RaycastAll(pointer, raycastResults);
        if (raycastResults.Count > 0)
        {
            for (int i = 0; i < raycastResults.Count; i++)
            {
                if (raycastResults[i].gameObject.GetComponent<AnalogueButtons>())
                {
                    if (!raycastResults[i].gameObject.GetComponent<AnalogueButtons>().isSelected)
                    {
                        selectedButtons.Add(raycastResults[i].gameObject.GetComponent<AnalogueButtons>());
                        raycastResults[i].gameObject.GetComponent<AnalogueButtons>().OnSelect();
                    }
                }
            }
        }

        if(Input.GetAxis("TriggerSelect") >= 1)
        {
            if (raycastResults.Count > 0)
            {
                for (int i = 0; i < raycastResults.Count; i++)
                {
                    if (raycastResults[i].gameObject.GetComponent<AnalogueButtons>())
                        raycastResults[i].gameObject.GetComponent<AnalogueButtons>().OnClick();
                }
            } 
        }

        foreach (AnalogueButtons button in selectedButtons)
        {
            if (!button.clicked)
                button.OnDeselect();
        }

        selectedButtons.Clear();

	}
}