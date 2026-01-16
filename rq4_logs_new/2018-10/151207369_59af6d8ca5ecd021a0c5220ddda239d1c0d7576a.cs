using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public abstract class BaseEnemy : MonoBehaviour {

    public float Health = 250f;
    public float Armor = 10f;
    public float Mana = 100f;
    public float Strength = 5f;
    public float Dexterity = 5f;
    public float Intelligence = 5f;
    public float Endurance = 5f;
    public float aggroRange = 5f;

    public void Attack()
    {
        //Do Damage to attacked person
    }

    public void Walk()
    {

    }
}