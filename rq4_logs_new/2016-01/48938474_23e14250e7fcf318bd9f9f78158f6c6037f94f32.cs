using UnityEngine;
using System.Collections;

public class Player1Mov : MonoBehaviour {

    private float rotLerpRate = 5;
    
    // Use this for initialization
	void Start () {
	
	}
	
	// Update is called once per frame
	void Update () {
        //GetComponent<Transform>().position = (new Vector3(Input.GetAxis("Player1_Horizontal"), 0, Input.GetAxis("Player1_Vertical"))) + GetComponent<Transform>().position;
        if (Input.GetAxis("Player1_Acceleration") > 0)
        {
            GetComponent<Rigidbody>().AddForce(transform.up * Input.GetAxis("Player1_Acceleration"), ForceMode.Force);
        }
        //Debug.Log(Input.GetAxis("Player1_Acceleration"));

        
       
        if (Input.GetAxis("Player1_Vertical") >= 0)
        {
            GetComponent<Transform>().rotation = Quaternion.Lerp(GetComponent<Transform>().rotation, Quaternion.Euler(90, 0, Mathf.Rad2Deg * Mathf.Asin(Input.GetAxis("Player1_Horizontal") * -1)), Time.deltaTime * rotLerpRate); //Mathf.Asin(Input.GetAxis("Player1_Horizontal"))
            transform.rotation = Quaternion.Euler(new Vector3(90, transform.rotation.eulerAngles.y, transform.rotation.eulerAngles.z));
        }
        else
        {
            GetComponent<Transform>().rotation = Quaternion.Lerp(GetComponent<Transform>().rotation, Quaternion.Euler(270, 0, Mathf.Rad2Deg * Mathf.Asin(Input.GetAxis("Player1_Horizontal") * -1)), Time.deltaTime* rotLerpRate); //Mathf.Asin(Input.GetAxis("Player1_Horizontal"))
            transform.rotation = Quaternion.Euler(new Vector3(270, transform.rotation.eulerAngles.y, transform.rotation.eulerAngles.z));
        }

        

    }
}