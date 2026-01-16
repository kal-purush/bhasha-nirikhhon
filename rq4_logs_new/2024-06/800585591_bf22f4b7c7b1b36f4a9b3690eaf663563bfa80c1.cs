using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class TradeSpawnPoint : MonoBehaviour
{
    public void SpawnIngredient(GameObject ingredient)
    {
        if (ingredient != null)
        {
            var spawned = Instantiate(ingredient, transform.position, transform.rotation);
            Rigidbody rb = spawned.GetComponent<Rigidbody>();
            if (rb != null)
            {
                rb.AddForce(transform.up * 3, ForceMode.Impulse);
            }
        }
    }
}