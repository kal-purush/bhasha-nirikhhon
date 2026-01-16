using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ItemQuest : MonoBehaviour
{
    public string condition = "";
    public string open = "";
    public Vector3 q1 = new Vector3();

    public void quest(string con, string get , Vector3 v1)
    {
        if (GameObject.Find("Mary").GetComponent<Inventory>().items.Contains(con))
        {
            if (GameObject.Find(get))
            {
                GameObject.Find(get).GetComponent<Transform>().position = v1;
            }
        }
    }

    // Update is called once per frame
    void Update()
    {
        quest(condition, open, q1);
    }
}