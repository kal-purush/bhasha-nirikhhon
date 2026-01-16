using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class NukeEnemies : MonoBehaviour
{
    private GameObject[] enemies;
    // Start is called before the first frame update
    void Start()
    {
        enemies = GameObject.FindGameObjectsWithTag("Enemy");
    }

    // Update is called once per frame
    void Update()
    {
        
    }


    void OnTriggerEnter(Collider other) {
        enemies = GameObject.FindGameObjectsWithTag("Enemy");

        if (other.gameObject.CompareTag("Nuke")) {
            foreach (GameObject enemy in enemies)
            {
                enemy.GetComponent<EnemyHealth>().TakeDamage(100000);
            }
            Destroy(other.gameObject);
        }
    }
    
    
}