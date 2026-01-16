using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class NPC : MonoBehaviour, IAttack
{
    [SerializeField] private int damage = 1;
    public PlayerStats stats;

    public Vector2 KnockbackForce { get => throw new System.NotImplementedException(); set => throw new System.NotImplementedException(); }
    public int Damage { get => throw new System.NotImplementedException(); set => throw new System.NotImplementedException(); }
    public BoxCollider2D Hitbox { get => throw new System.NotImplementedException(); set => throw new System.NotImplementedException(); }
}