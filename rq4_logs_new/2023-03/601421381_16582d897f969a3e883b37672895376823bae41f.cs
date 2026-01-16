using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class behavior1 : MonoBehaviour
{
    //Class variable to be changed from the Unity interface
    [SerializeField] GameObject ball;
    [SerializeField] float delay;
    // Start is called before the first frame update
    void Start()
    {
        //call the "dropball" function after 0.5 seconds
        //and then keep calling it every 0.8 seconds
        InvokeRepeating("DropBall", 0.5f, delay);
    }

    // Update is called once per frame
    void Update()
    {
        
    }

    void DropBall()
    {
        Vector3 pos = new Vector3(Random.Range(-30.0f, 30.0f), 25, 0);
        //Create a copy of the prefab
        GameObject obj = Instantiate(ball, pos, Quaternion.identity);
        //Doom the object to die in 5 seconds
        Destroy(obj, 5);
    }
}