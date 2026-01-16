using UnityEngine;

public class GoblinGateTrigger : MonoBehaviour
{
    public Transform[] spawnPoints;

    private bool hasSpawned = false;

    public GameObject[] monsters; 

    private void OnTriggerEnter2D(Collider2D other)
    {
        if (other.CompareTag("Player") && !hasSpawned)
        {
            hasSpawned = true;
            foreach (Transform t in spawnPoints)
            {
                GameObject randomMonster = monsters[Random.Range(0, monsters.Length)];
                Instantiate(randomMonster, t.position, Quaternion.identity);
            }
        }
    }


}