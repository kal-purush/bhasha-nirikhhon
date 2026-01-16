using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.Tilemaps;

public class HugosScript : MonoBehaviour
{

    public Tilemap tilemap;
    // Start is called before the first frame update
    void Start()
    {
        
    }

    // Update is called once per frame
    void Update()
    {
    }
private void OnCollisionStay2D(Collision2D other) {
    Debug.Log(other.contactCount);
    }

        
}