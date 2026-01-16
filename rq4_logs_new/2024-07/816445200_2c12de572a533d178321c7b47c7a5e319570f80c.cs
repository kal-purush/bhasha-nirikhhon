using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class BarracksWall : MonoBehaviour
{
    [SerializeField] private GameObject wall;

    private bool isPlayerIn;

    private void OnTriggerEnter(Collider other)
    {
        if (other.gameObject.CompareTag("Player"))
        {
            isPlayerIn = true;
        }
    }

    private void OnTriggerExit(Collider other)
    {
        if (other.gameObject.CompareTag("Player"))
        {
            isPlayerIn = false;
        }
    }

    private void Update()
    {
        if (isPlayerIn) {
            wall.SetActive(false);
        }
        else
        {
            wall.SetActive(true);
        }
    }

}