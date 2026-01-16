using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Level4_Lever : MonoBehaviour
{
    [SerializeField] GameObject lockPosition;
    [SerializeField] GameObject openPosition;

    public bool isOpened = false;
    public GameObject door;

    private void Start()
    {
        if (!isOpened)
        {
            lockPosition.SetActive(true);
            openPosition.SetActive(false);
        }
        else
        {
            Debug.Log("This lever is opened at the beginning of the game?");
        }

    }
    public void OpenLever()
    {
        isOpened = true;
        lockPosition.SetActive(false);
        openPosition.SetActive(true);
        door.SetActive(false);
    }


    private void OnTriggerEnter(Collider other)
    {
        if (other.gameObject.tag == "Player")
        {
            Debug.Log("Player enter");
            OpenLever();
        }
    }
}