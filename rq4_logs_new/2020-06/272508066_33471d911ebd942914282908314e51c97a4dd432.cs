using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ResourceStack : MonoBehaviour
{
    public Transform ResourceInstance;
    public float GapSize = 0.1f;
    public int StackSize = 7;
    public int StackWidth = 2;
    public float InstanceWidth = 1;
    public float InstanceHeight = 1;


    // Start is called before the first frame update
    void Start()
    {


        if (ResourceInstance)
        {

        }

        if (ResourceInstance)
        {
            for (int i = 0; i < StackSize; i++)
            {
                Debug.Log((InstanceWidth + GapSize) * i);
//                Instantiate(ResourceInstance, new Vector3((InstanceWidth + GapSize) * i, 0, 0), Quaternion.identity, transform);
                Transform resourcePile = Instantiate(ResourceInstance,  transform);
                resourcePile.localPosition = new Vector3((InstanceWidth + GapSize) * i, 0, 0);

            }
        }
    }

    // Update is called once per frame
    void Update()
    {
        
    }
}