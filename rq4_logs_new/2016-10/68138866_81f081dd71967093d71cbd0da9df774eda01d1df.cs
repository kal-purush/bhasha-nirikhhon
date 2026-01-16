using UnityEngine;
using System.Collections;

public class SpinCommand : MonoBehaviour
{
    public AudioClip otherClip;
   // AudioSource audio;
    public bool clickSpin = true;
    private GameObject Red;
    private GameObject Yellow;
    private GameObject Green;
    private GameObject Blue;
    void Start()
    {
    }
    void Update()
    {
        if (clickSpin)
        {
            Red.GetComponent<Renderer>().material.color = Color.red;
            Red.transform.Rotate(Vector3.up, Time.deltaTime * 30, Space.World);
        }
        else
        {
            Red. GetComponent<Renderer>().material.color = Color.white;
        }
    
    if (clickSpin)
        {
            Yellow.GetComponent<Renderer>().material.color = Color.yellow;
            Yellow.transform.Rotate(Vector3.up, Time.deltaTime* 30, Space.World);
        }
        else
        {
            Yellow.GetComponent<Renderer>().material.color = Color.white;
        }
        if (clickSpin)
        {
            Green.GetComponent<Renderer>().material.color = Color.green;
            Green.transform.Rotate(Vector3.up, Time.deltaTime * 30, Space.World);
        }
        else
        {
            Green.GetComponent<Renderer>().material.color = Color.white;
        }

        if (clickSpin)
        {
            Blue.GetComponent<Renderer>().material.color = Color.blue;
           Blue.transform.Rotate(Vector3.up, Time.deltaTime * 30, Space.World);
        }
        else
        {
            Blue.GetComponent<Renderer>().material.color = Color.white;
        }
    }
}