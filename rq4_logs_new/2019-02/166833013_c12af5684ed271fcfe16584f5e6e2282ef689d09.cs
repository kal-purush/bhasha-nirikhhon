using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class DestroyWall : MonoBehaviour
{
    // Start is called before the first frame update
    void Start()
    {
        
    }

    // Update is called once per frame
    void Update()
    {
        if (Input.GetMouseButtonDown(0))
        {
            Vector2 p = Input.mousePosition;
            //p.y = Screen.height - p.y; 
            Ray ray = Camera.main.ScreenPointToRay(p);
            RaycastHit hit;
            if (Physics.Raycast(ray, out hit))
            {
                if (hit.collider.tag == "wall")
                {
                    Debug.Log("delete");
                    Destroy(gameObject);
                }
            }
        }
    }

    void OnCollisionEnter(Collision otherObj)
    {
        if (otherObj.gameObject.tag == "wall")
        {
            Destroy(gameObject, .5f);
        }
    }
}
