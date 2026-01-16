using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class CanUseitem : MonoBehaviour
{
    public GameObject defaultBox;
    public GameObject itemBox;
    public bool canUseItem = false;
    // Use this for initialization
    void Start()
    {

    }

    // Update is called once per frame
    void Update()
    {

    }
    public void activatedBox()
    {
        if (canUseItem)
        {
            itemBox.SetActive(true);
        }
        else
            defaultBox.SetActive(true);
    }

    void OnTriggerEnter2D(Collider2D other)
    {
        Debug.Log("enter");
        if (other.gameObject.tag == "Player")
        {
            canUseItem = true;
        }
    }

    void OnTriggerExit2D(Collider2D other)
    {
        if (other.gameObject.tag == "Player")
        {
            canUseItem = false;
        }
    }
}