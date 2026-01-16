using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class NewBehaviourScript : MonoBehaviour
{

    public float GameTimer = 0.0f;
    public GameObject MonsterGo;
    // Start is called before the first frame update
    void Start()
    {
       
    }

    // Update is called once per frame
    void Update()
    {
        GameTimer -= Time.deltaTime;

        if (GameTimer <= 0)
        {
            GameTimer = 3.0f;

            GameObject Temp = Instantiate(MonsterGo);
            Temp.transform.position =  new Vector3(Random.Range(-10, 10), Random.Range(-4, 4), 0.0f); 
        }






        if (Input.GetMouseButtonDown(0))
        {
            RaycastHit hit;
            Ray ray = Camera.main.ScreenPointToRay(Input.mousePosition);

            if (Physics.Raycast(ray, out hit))
            {
                if (hit.collider != null)
                {
                    //Debug.Log(hit.collider.name);
                    hit.collider.gameObject.GetComponent<Monster>().CharactorHit(10);
                }
            }
        }
    }
}