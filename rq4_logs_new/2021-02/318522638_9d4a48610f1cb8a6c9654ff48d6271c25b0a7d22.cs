using Rhyth.BTree;
using System.Collections;
using UnityEngine;

/// <summary>
/// Used for creating an boss entity.
/// </summary>
[CreateAssetMenu(menuName = "Entity/Boss")]
public class BossObject : ScriptableObject
{
    public GameObject Prefab => prefab;
    [SerializeField] private GameObject prefab;

    public Weapon Weapon => weapon;
    [SerializeField] private Weapon weapon;

    public BTree BehaviourTree => behaviourTree;
    [SerializeField] private BTree behaviourTree;

    public EnemyStats Stats => stats;
    [SerializeField] private EnemyStats stats;
}