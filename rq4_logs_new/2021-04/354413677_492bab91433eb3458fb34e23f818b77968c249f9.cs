using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Breakable : MonoBehaviour
{
    public int maxHealth = 100;
    int currentHealth;

    public LootTable lootTable;
    [Range(0, 25)] [SerializeField] private int minDropQuantity = 0;
    [Range(0, 25)] [SerializeField] private int maxDropQuantity = 1;
    
    //Chance de drop (%)
    // Se o inimigo tem 50% na frequência de drop, 1 em cada 2 mortes (probabilidade)
    //serão sorteados os drops. Então se o inimigo tem 100% na frequência de drop,
    //todas as mortes sortearão drops.
    [Range(0, 100.0f)] [SerializeField] private float frequency = 100.0f;

    private TintColor tintColor;

    int realMaxDrop;

    bool isDead = false;

    void Start()
    {
        tintColor = GetComponent<TintColor>();
        currentHealth = maxHealth;

        realMaxDrop = maxDropQuantity + 1;
        if(maxDropQuantity < minDropQuantity)
        {
            Debug.Log("maxDrop less than minDrop (?)");
            maxDropQuantity = minDropQuantity;
        }
    }

    public void TakeDamage(int damage)
    {
        tintColor.SetTintColor(new Color(255, 0, 0, 255));
        currentHealth -= damage;
        
        if (currentHealth <= 0)
        {
            Die();
        }
    }
    
    private void Drop()
    {
        if (lootTable != null && lootTable.item != null && !isDead)
        {
            if(frequency >= Random.Range(0.0f, 100.0f))
            {
                int quantity;
                if(minDropQuantity == maxDropQuantity)
                {
                    quantity = minDropQuantity;
                }
                else
                {
                    quantity = Random.Range(minDropQuantity, realMaxDrop);
                }
                float xPos, yPos;
                GameObject droppedItem;
                xPos = GetComponent<Transform>().position.x;
                yPos = GetComponent<Transform>().position.y;
                for (int i = 0; i < quantity; ++i)
                {
                    droppedItem = Instantiate(lootTable.item, new Vector3(xPos, yPos, -1), Quaternion.identity);
                    droppedItem.SetActive(true);
                }
                Debug.Log("Dropado " + quantity + " itens");
            }
        }
    }

    private void Die()
    {
        Drop();
        Destroy(gameObject);
        isDead = true;
    }
}