using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Items : MonoBehaviour
{
   private PlayerController player;
    void Awake() {
        Collider[] colliders = GetComponents<Collider>();
        if (colliders.Length > 0) {
            foreach (Collider col in colliders) {
                col.isTrigger = true;
            }
        } else {
            Debug.LogWarning("No se encontraron Colliders en " + gameObject.name);
        }

        player = FindObjectOfType<PlayerController>();
    }
   
   void OnTriggerEnter(Collider collider) {
    if (collider.gameObject.CompareTag("Player")) {
        switch (gameObject.tag) {
            case "JumpReset":
                if (!player._doubleJump) {
                    player._doubleJump = true;
                }
                Destroy(gameObject);
                break;
            default:
                Destroy(gameObject);
                break;
        }
    }
}
}