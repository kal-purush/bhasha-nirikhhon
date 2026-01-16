using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class HealerAI : MonoBehaviour
{
    private GameObject[] shieldAllies;
    private float healSpeed = 8f;

    private void Update()
    {
        Heal();
    }

    public void Heal()
    {
        healSpeed -= Time.deltaTime;
        if (healSpeed <= 0f)
        {
            shieldAllies = GameObject.FindGameObjectsWithTag("Shield");

            foreach(GameObject shieldAlly in shieldAllies)
            {
                if (shieldAlly.activeInHierarchy == false)
                {
                    return;
                }
                shieldAlly.GetComponent<BlockerAI>().health = 100f;
                shieldAlly.GetComponent<BlockerAI>().healthBar.sizeDelta = new Vector2(100,20);
                healSpeed = 8f;
            }
        }
    }

    private void OnCollisionEnter2D(Collision2D other)
    {
        if (other.gameObject.tag == "Enemy")
        {
            Destroy(this.gameObject);
        }
    }
}