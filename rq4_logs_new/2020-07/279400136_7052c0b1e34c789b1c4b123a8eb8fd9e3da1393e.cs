using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.SceneManagement;

public class Interact : MonoBehaviour
{
    private float distance;
    private bool inArea = false;

    public GameObject player;
    public bool goal;

    void OnTriggerEnter(Collider other) {
        if (other.gameObject.layer == LayerMask.NameToLayer("Player")) {
            inArea = true;
            player.transform.GetChild(1).GetChild(1).gameObject.SetActive(inArea);
        }
    }

    void OnTriggerExit(Collider other) {
        if (other.gameObject.layer == LayerMask.NameToLayer("Player")) {
            inArea = false;
            player.transform.GetChild(1).GetChild(1).gameObject.SetActive(inArea);
        }
    }

    void Update() {
        if (inArea && Input.GetMouseButtonDown(0)) {
            if (goal) {
                Debug.Log("YOU WIN");
                // SceneManager.LoadScene("Corridor", LoadSceneMode.Additive);
            } else {
                Debug.Log("YOU LOSE");
                // SceneManager.LoadScene("Corridor", LoadSceneMode.Additive);
            }
        }
        
    }
}