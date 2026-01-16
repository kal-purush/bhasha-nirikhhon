using System.Collections;
using System.Collections.Generic;
using UnityEngine;
 
public class Cube : MonoBehaviour
{
    [SerializeField]
    private float _moveParameter;

    private float _initY;

    void Start()
    {
        _initY = transform.position.y;
    }

    void Update()
    {
        float dx = Input.GetAxis("Horizontal") * Time.deltaTime * _moveParameter;
        float dz = Input.GetAxis("Vertical") * Time.deltaTime * _moveParameter;
        transform.position = new Vector3 (
        transform.position.x + dx, _initY, transform.position.z + dz
        );
    }
}