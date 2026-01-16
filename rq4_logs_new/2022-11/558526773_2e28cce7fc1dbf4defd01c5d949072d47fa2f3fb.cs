using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ExecutionBlockBuilder : MonoBehaviour
{
    // Start is called before the first frame update
    void Start()
    {
        // tu je priklad instanciovania prefabu z Recourses, tieto hodnoty sa budu menit pri detekcii
        // nieco take ze prides do lifeline, cez sipky pozries ci koncia/zacinaju, podla toho tvoris tieto blocky..
        GameObject g = Instantiate(Resources.Load("ExecBlock") as GameObject, transform.position, Quaternion.identity);
        g.transform.parent = transform;
        Vector3 pos = g.transform.position;
        pos.y -= 100F;
        g.transform.position = pos;
    }

    // Update is called once per frame
    void Update()
    {
        
    }
}