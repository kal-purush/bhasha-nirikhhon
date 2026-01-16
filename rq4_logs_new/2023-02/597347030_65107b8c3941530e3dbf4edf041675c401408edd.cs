using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class BaseCollision : MonoBehaviour
{
    bool inBase;
    Renderer render;
    public string BaseName;
    WorkerInventory inventory;
    // Start is called before the first frame update
    void Start()
    {
        inBase = true;
        render = GetComponent<Renderer>();
        inventory = GetComponent<WorkerInventory>();
    }

    // Update is called once per frame
    void Update()
    {
        
    }

    void OnCollisionExit(Collision other)
    {
        Debug.Log("start collision with " + other.transform.name);
        if (other.transform.name != BaseName)
            return;
        inBase = false;
        render.material.color = Color.white;
        
    }

    void OnCollisionEnter(Collision other)
    {
        Debug.Log("end collision with " + other.transform.name);
        if (other.transform.name != BaseName)
            return;

        WorkerInventory baseInventory = other.gameObject.GetComponent<WorkerInventory>();
        int[] workerInventory = inventory.getInventory();
        int[] baseStock = baseInventory.getInventory();
        for (int i = 0; i != workerInventory.Length; ++i) {
            baseInventory.addItem(i, workerInventory[i]);
            inventory.addItem(i, -workerInventory[i]);
        }
        inBase = true;
        render.material.color = Color.green;
    }
}