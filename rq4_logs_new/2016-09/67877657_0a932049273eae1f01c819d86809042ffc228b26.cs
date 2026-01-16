using UnityEngine;
using System.Collections;

public class AttachToController : MonoBehaviour {

    public GameObject vrControllerModel;

	// Use this for initialization
	void Start () {
	
	}
	
	// Update is called once per frame
	void Update () {
        try
        {
            Transform attachObject = vrControllerModel.transform.Find("tip").transform.Find("attach");
            transform.position = attachObject.position;
        } catch (System.NullReferenceException exception)
        {
        }
	}
}