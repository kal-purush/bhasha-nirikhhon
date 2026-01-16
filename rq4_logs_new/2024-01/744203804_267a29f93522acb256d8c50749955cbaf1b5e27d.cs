using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class AttackManager : MonoBehaviour
{
    public GameObject prefab;
    public GameObject cursor;
    public PlayerController player;

    public List<GameObject> inventory;
    private List<float> timers = new List<float>();
    private int numOfWeapons = 0;
    // Start is called before the first frame update
    void Start()
    {
        player = FindAnyObjectByType<PlayerController>();

    }

    private void Update()
    {
        if (numOfWeapons != inventory.Count)
        {
            timers.Add(inventory[numOfWeapons].GetComponent<WeaponStats>().timer);
            numOfWeapons++;
        }
    }


    // Update is called once per frame
    void FixedUpdate()
    {
        for (int i = 0; i < timers.Count; i++)
        {
            if (timers[i] <= 0)
            {
                Fire(inventory[i]);
                timers[i] = inventory[i].GetComponent<WeaponStats>().timer;
            }
            else
            {
                timers[i] = timers[i] -= 0.01f;
            }
        }
    }


    void Fire(GameObject attack)
    {
        GameObject bullet = Instantiate(attack, player.transform.position, FindAnyObjectByType<CursorController>().pointerRotation * Quaternion.Euler(0, 0, -45));
        SetDirectionBasedOnDifferentWeapon(bullet);
    }

    void SetDirectionBasedOnDifferentWeapon(GameObject bullet)
    {
        if (bullet.GetComponent<Kunai>() != null)
        {
            bullet.GetComponent<Kunai>().SetTarget(-FindAnyObjectByType<CursorController>().throwingDirection.normalized);
        }
        if (bullet.GetComponent<LargeShuriken>() != null)
        {
            bullet.GetComponent<LargeShuriken>().SetTarget(-FindAnyObjectByType<CursorController>().throwingDirection.normalized);
        }
    }

    void FireMultiple()
    { 
        
    }
}