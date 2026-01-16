using System.Collections;
using System.Collections.Generic;
using UnityEngine;

[System.Serializable]
public class WaterCanon : MonoBehaviour
{
    [SerializeField]public GameObject WaterHose;
    [SerializeField]public GameObject Ball;
    private Vector3 bulletOffst = new Vector3(1, 1, 1);
    private int Velocity;
    private bool IsFiring { get; set; }
    // Start is called before the first frame update
    void Start()
    {
        
    }

    // Update is called once per frame
    void Update()
    {
        if(Input.GetKey("s")) 
        {
            Shoot();
        }
    }
    void Shoot()
    {
        // Place bullet at the end of the gun
        Vector3 pos = WaterHose.transform.position;
        Vector3 ofst = WaterHose.transform.forward;
        ofst.Scale(bulletOffst);
        pos = pos + ofst;

        // Instantiate bullet
        Instantiate(Ball, pos, Quaternion.identity);
    }
}