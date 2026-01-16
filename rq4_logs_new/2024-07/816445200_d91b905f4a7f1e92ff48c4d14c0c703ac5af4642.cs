using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class LibraryDoor : MonoBehaviour
{
    [SerializeField] private Transform spawnPoint;
    [SerializeField] private GameObject libraryRoof;
    [SerializeField] private bool roofActive;


    private void OnTriggerEnter(Collider other)
    {
        if (other.gameObject.CompareTag("Player"))
        {
            other.gameObject.transform.position = spawnPoint.position;
            libraryRoof.SetActive(roofActive);
        }
    }
}