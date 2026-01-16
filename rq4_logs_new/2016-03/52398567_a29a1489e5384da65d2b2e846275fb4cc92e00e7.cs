using UnityEngine;
using UnityEngine.UI;
using System.Collections;

public class Interaktiv : MonoBehaviour {

    new private Camera camera;

    public Text kijelzo;

	void Start () {
        camera = GetComponentInChildren<Camera>();	
	}
	
	void Update () {

        RaycastHit hit;
        Ray ray = camera.ScreenPointToRay(Input.mousePosition);

        if (Physics.Raycast(ray, out hit))
        {
            Transform objectHit = hit.transform;
            if (kijelzo)
            {
                kijelzo.text = "Fókusz: " + objectHit.name;
            }
            // Do something with the object that was hit by the raycast.
        }

    }
}