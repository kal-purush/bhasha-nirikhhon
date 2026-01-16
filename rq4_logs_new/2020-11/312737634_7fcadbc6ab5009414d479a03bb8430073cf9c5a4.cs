using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class PivotRotation : MonoBehaviour
{
    private GameObject botao;
    void Start()
    {
        botao=Resources.Load("button") as GameObject;
        Vector3 rotation=new Vector3(0,0,6);
        for(int i=0;i<65;i++){
            Instantiate(botao,new Vector3(0,4,0),Quaternion.identity, transform);
            transform.Rotate(rotation,Space.Self);            
        }
    }

    void Update()
    {
        Vector3 rotation=new Vector3(0,0,1*Time.deltaTime);
        transform.Rotate(rotation,Space.Self);
    }
}