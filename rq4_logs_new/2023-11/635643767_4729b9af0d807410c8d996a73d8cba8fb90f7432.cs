using UnityEngine;

public class PrefabBulletSphere : MonoBehaviour
{
    [SerializeField] private Transform shootingPoints = default;
    [SerializeField] private Transform pfProjectile = default;

    private Transform projectileParent = default;

    private float speed = 2.0f;
    private float bulletSpeed = 4.0f;
    private readonly float cooldown = 1.5f;
    private float cooldownTimer = default;

    //===========================================================================
    private void OnTriggerEnter2D(Collider2D collision)
    {
        if (SceneControlManager.Instance.CurrentGameplayState == GameplayState.Pause)
            return;

        if (collision.CompareTag("Player"))
        {
            Player.Instance.GetComponent<PlayerHeart>().UpdateCurrentHeart(-3);

            gameObject.SetActive(false);
            Destroy(gameObject);
        }

        if (collision.CompareTag("Collisions"))
        {
            gameObject.SetActive(false);
            Destroy(gameObject);
        }
    }

    //===========================================================================
    private void Start()
    {
        cooldownTimer = cooldown;

        projectileParent = GameObject.Find("Prefabs").transform;
    }

    private void Update()
    {
        if (SceneControlManager.Instance.CurrentGameplayState == GameplayState.Pause)
            return;

        transform.Translate(Vector3.down * Time.deltaTime * speed);

        // Cooldown
        cooldownTimer -= Time.deltaTime;
        if (cooldownTimer <= 0.0f)
        {
            cooldownTimer = cooldown;
            ShootBullet();
        }
    }

    //===========================================================================
    private void ShootBullet()
    {
        foreach (Transform shootingPoint in shootingPoints)
        {
            Transform _projectile = Instantiate(pfProjectile, projectileParent);
            _projectile.position = transform.position;

            Vector2 _moveDirection = (shootingPoint.position - transform.position).normalized;
            _projectile.GetComponent<EnemyProjectile>().SetMoveDirectionAndSpeed(_moveDirection, bulletSpeed);
        }
    }
}