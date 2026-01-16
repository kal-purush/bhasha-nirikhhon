using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class DropLoot : MonoBehaviour
{
    [SerializeField] private GameObject dropType; // GameObject to spawn
    private List<GameObject> spawnedDrops = new List<GameObject>(); // List to keep track of spawned drops
    private bool isQuitting = false;

    // Called when the GameObject is destroyed
    void OnDestroy()
    {
        // Spawn drop only if the application is not quitting
        if (isQuitting == false)
        {

            GameObject drop = Instantiate(dropType, transform.position, Quaternion.identity);
            spawnedDrops.Add(drop);
        }
    }

    // Called when the application is quitting
    private void OnApplicationQuit()
    {
        isQuitting = true;
        // Destroy all spawned drops if the application is quitting
        foreach (GameObject drop in spawnedDrops)
        {
            Destroy(drop);
        }
        // Clear the list of spawned drops
        spawnedDrops.Clear();
    }
}