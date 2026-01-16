using UnityEngine;

public class FastDistantAttack : MonoBehaviour
{
    public float timeout = 10;

    public float projectileSpawnDistance = 1;

    public float viewRange = 10;

    public Vector3 spawnOffset;
    
    public Projectile projectile;

    private float _lastAttackTime;
    
    private Transform _player;

    public int countOfProjectiles = 7;


    public float betweenFireTime = 0.3f;



    private void Start()
    {
        _player = FindObjectOfType<PlayerTag>().transform;
    }

    private void Update()
    {
        if (_lastAttackTime + timeout > Time.time)
        {
            return;
        }

        if ((_player.position - transform.position).sqrMagnitude > viewRange * viewRange)
        {
            return;
        }



        for (int i = 0; i != countOfProjectiles; ++i)
        {
            Invoke("Fire", betweenFireTime * i);
        }
        
        _lastAttackTime = Time.time;
    }

    private void Fire()
    {
        Vector3 pos = transform.position;
        Vector3 playerPos = _player.position;

        Vector3 toPlayer = (playerPos - pos).normalized;
        
        Projectile spawnedProjectile = Instantiate(
            projectile, 
            pos + spawnOffset + toPlayer * projectileSpawnDistance, 
            Quaternion.FromToRotation(Vector3.right, toPlayer));

        spawnedProjectile.direction = toPlayer.normalized;
        spawnedProjectile.transform.localScale = new Vector3(0.5f, 0.5f, 1.0f);
    }
}