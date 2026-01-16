using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class CameraPosition : MonoBehaviour
{
    public Transform Camera;
    public Transform Player;
    [SerializeField] float minY;
    [SerializeField] float maxY;
    [SerializeField] float minX;
    [SerializeField] float maxX;
    // Start is called before the first frame update
    void Start()
    {
        
    }

    // Update is called once per frame
    void Update()
    {
        float xClamp = Mathf.Clamp(Player.position.x, minX, maxX);
        float yClamp = Mathf.Clamp(Player.position.y, minY, maxX);
        Camera.position = new Vector3(xClamp, yClamp, Camera.position.z);
    }
}