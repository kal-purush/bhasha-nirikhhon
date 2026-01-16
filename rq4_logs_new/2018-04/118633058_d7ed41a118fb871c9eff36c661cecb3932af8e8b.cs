using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Entity : MonoBehaviour {

    float health = 100f;
    float maxHealth = 100f;

    GameObject entity;

    public SpriteRenderer weapon;
    public Inventory inventory;

    private SpriteRenderer spriteRenderer;
    private Animator animator;


    // Use this for initialization
    void Start () {
        entity = GetComponent<GameObject>();
        inventory.ItemUsed += Inventory_ItemUsed;
    }
	
	// Update is called once per frame
	void Update () {
		
	}

    public void modifyHealth(float modifier) {
        health = health + modifier;
        if(isMaxHealthExceeded())
        {
            health = maxHealth;
        }
        else if(!isAlive())
        {
            die();
        }

    }

    public void modifyMaxHealth(float modifier)
    {
        maxHealth += modifier;
    }

    private bool isMaxHealthExceeded()
    {
        return health > maxHealth;
    }

    private bool isAlive()
    {
        return health > 0f;
    }

    private void die () {
        Destroy(entity);
    }

    private void Inventory_ItemUsed(object sender, InventoryEventArgs e)
    {
        ItemBaseClass item = e.Item;
        weapon.sprite = item.Image;
    }
}