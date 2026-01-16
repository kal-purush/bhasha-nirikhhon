using UnityEngine;

public class BossManager : MonoBehaviour
{
    public static BossManager Instance { get; private set; }
    [SerializeField] private int _enemyKillCount;

    private void Awake()
    {
        if (Instance != null && Instance != this)
        {
            Destroy(this.gameObject);
        }
        else
        {
            Instance = this;
        }
    }

    private void Start()
    {
        _enemyKillCount = 0;
    }

    public void ResetKillCount()
    {
        _enemyKillCount = 0;

    }

    public int GetEnemyKillCount()
    {
        return _enemyKillCount;
    }
    public void IncrementEnemyKillCount()
    {
        _enemyKillCount++;
    }

}