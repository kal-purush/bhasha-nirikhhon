using UnityEngine;

public class BasicTowerInfo : MonoBehaviour
{
    public enum DetectionMode
    {
        Nearest,
        Farthest,
        Strongest,
    }

    public Collider _dectectionRadius;
    //public GameObject bulletPrefab;
    public TowerStats Stats;
    public TowerType towerType;
    //public GameObject playerModel;

    public DetectionMode detectionMode;

    [Header("Direct")]
    //public Transform firepoint;
    public float maxBubble;
    public float currentBubble;
    public float bubbleHealthDeduct;
    public float bubbleGainAmount;
    public float bubbleCost;

    void Awake()
    {
        _dectectionRadius = GetComponent<CapsuleCollider>();
    }
}