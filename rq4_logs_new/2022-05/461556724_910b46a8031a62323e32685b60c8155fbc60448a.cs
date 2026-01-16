using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class cube : MonoBehaviour
{
    public GameObject[] cubes;
    public Dictionary<int, GameObject> dic = new Dictionary<int, GameObject>();

    RaycastHit hit;
    int count;

    private void Start()
    {
        for(int i = 0; i < cubes.Length; i++)
        {
            dic.Add(i, cubes[i]);
        }
    }

    private void Update()
    {
        if(Input.GetMouseButtonDown(0))
        {
            Ray ray = Camera.main.ScreenPointToRay(Input.mousePosition);

            if(Physics.Raycast(ray, out hit))
            {
                if(GetNumber(hit).Equals(count))
                {
                    count++;
                    hit.collider.gameObject.GetComponent<Renderer>().material.color = Color.blue;
                }
            }
        }
    }

    int GetNumber(RaycastHit hitInfo)
    {
        foreach(var item in dic)
        {
            if(item.Value.Equals(hitInfo.collider.gameObject))
            {
                return item.Key;
            }
        }
        return -1;
    }
}