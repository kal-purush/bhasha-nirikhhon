using UnityEngine;
using System.Collections;

public class KillZone : MonoBehaviour
{
    private playerStats player_stats;

	void Start()
    {
	
	}
	
	void Update()
    {
	
	}

    void OnTriggerEnter2D(Collider2D other)
    {
        if (other.tag != "Player")
            return;

        player_stats.Respawn();
    }
}