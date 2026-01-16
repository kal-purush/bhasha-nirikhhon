using System.Collections;
using System.Collections.Generic;
using UnityEngine;


public enum projectileType
{
    blueProjectile, redProjectile
};

public class Projectiles : MonoBehaviour
{
    [SerializeField]
    private int attackStrenght;
    [SerializeField]
    private projectileType projectileType;

    public int AttackStrength
    {
        get
        {
            return attackStrenght;
        }
    }

    public projectileType ProjectileType
    {
        get
        {
            return projectileType;
        }
    }
}