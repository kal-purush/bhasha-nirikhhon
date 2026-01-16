using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class invisibleGrid : MonoBehaviour
{
    // BoxCollider2D box;
    Grid rend;
    //bool pass = false;
    // Start is called before the first frame update
    void Start()
    {
        // box = this.gameObject.GetComponent<BoxCollider2D>();
        rend = this.gameObject.GetComponent<Grid>();
    }

    private void OnTriggerEnter2D(Collider2D collision)
    {
        if (collision.gameObject.name.Equals("Player"))
        {
            //pass = true;
            //if (pass)
            //{
            rend.enabled = !rend.enabled;
            // box.enabled = !box.enabled;
            //  }
        }
    }
    // Update is called once per frame
    void Update()
    {
        
    }
}