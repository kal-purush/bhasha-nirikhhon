using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ToolSource : MonoBehaviour
{

    public int toolId;

    public GameObject prefab;

    GameObject heldTool;

    // Start is called before the first frame update
    void Start()
    {
    }

    // Update is called once per frame
    void Update()
    {
        if (heldTool == null || heldTool.transform.parent != transform)
        {
            heldTool = Instantiate(prefab, transform.position, Quaternion.identity);
            heldTool.GetComponent<ProductInstance>().id = toolId;
            heldTool.transform.SetParent(transform);
        }
    }

}