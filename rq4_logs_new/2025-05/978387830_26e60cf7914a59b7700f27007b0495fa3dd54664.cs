using System.Collections;
using UnityEngine;

public class BossShooter : MonoBehaviour
{
    [SerializeField] private GameObject darkFlameBallPrefab;
    [SerializeField] private Transform firePoint;
    [SerializeField] private float shootInterval = 2f;
    [SerializeField] private float shootDirection = -1f;

    private void Start()
    {
        StartCoroutine(ShootRepeatedly());
    }

    private IEnumerator ShootRepeatedly()
    {
        while (true)
        {
            Shoot();
            yield return new WaitForSeconds(shootInterval);
        }
    }

    private void Shoot()
    {
        GameObject projectile = Instantiate(darkFlameBallPrefab, firePoint.position, Quaternion.identity);

        ProjectileEnemy projScript = projectile.GetComponent<ProjectileEnemy>();
        if (projScript != null)
        {
            projScript.SetDirection(shootDirection);
        }
    }
}