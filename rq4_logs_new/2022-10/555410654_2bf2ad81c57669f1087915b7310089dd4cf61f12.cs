using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Main : MonoBehaviour
{

    public GameObject planePrefab;
    public GameObject cubePrefab;




    // Start is called before the first frame update
    void Start()
    {

        //GameObject newObject = Instantiate(cube, plane.transform);
        //newObject.transform.position = new Vector3(0.0f, 0.0125f, 0.0f);


    }


    void Update()
    {

        if (Input.GetMouseButtonDown(0))
        {

            RaycastHit hit;
            Camera cam = Camera.main;
            Ray ray = cam.ScreenPointToRay(Input.mousePosition);

            bool wasHit = Physics.Raycast(ray.origin, ray.direction.normalized, out hit);

            if (wasHit)
            {
                GameObject hitGameObject = hit.transform.gameObject;
                if (hitGameObject.tag == "Pipes") {
                    hitGameObject.transform.Rotate(new Vector3(0,90.0f,0));
                    //TODO zmeni sa orientacia
                    hitGameObject.GetComponent<HorizontalPipe>().inputOrientation = 5;
                }


            }
        }
    }
}