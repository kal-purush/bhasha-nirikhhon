using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class DirectionalArrow : MonoBehaviour
{

    [SerializeField] private Transform targetKey;



    // Start is called before the first frame update
    void Start()
    {
        
    }

    // Update is called once per frame
    void Update()
    {
        Vector3 targetPosition = targetKey.transform.position;
        targetPosition.y = this.transform.position.y;
        transform.LookAt(targetPosition);
    }
}