using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class followPlayer : MonoBehaviour
{

    Transform playerTransform;

    [Range(8,20)]
    public float offSet;
    // Start is called before the first frame update
    void Start()
    {
        playerTransform = FindObjectOfType<playerMovement>().myTF;
    }

    // Update is called once per frame
    void Update()
    {
        setPos();
    }

    void setPos()
    {
        this.transform.position = new Vector3(playerTransform.position.x + offSet, this.transform.position.y, this.transform.position.z);
    }
}