using UnityEngine;
using System.Collections;

public class StartFight : MonoBehaviour {

    void OnTriggerEnter2D(Collider2D other)
    {
        if(other.name == "Player")
        {
            GameObject.Find("Boss1").GetComponent<BossAI>().Fight();
            gameObject.active = false;
        }
    }
}