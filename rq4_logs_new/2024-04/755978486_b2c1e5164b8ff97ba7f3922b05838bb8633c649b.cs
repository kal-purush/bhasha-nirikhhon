using Assets.Scripts.Effect;
using UnityEngine;

namespace Assets.Scripts.Combat
{
    [RequireComponent(typeof(Controller))]

    public class ShotGun : Gun
    {
        [SerializeField] private int _bulletCount = 3;

        public override void Shoot(Vector2 target)
        {
            if (HackEventManager.Instance != null && HackEventManager.Instance.IsHacking)
            {
                return;
            }

            // Screen Shake if bullet is big
            if (_bulletPrefab.transform.localScale.x >= 0.3)
            {
                ScreenShake.Shake(ScreenShake.ShakeType.ShootBigBullet);
            }

            _shootTimer = _shootingDelay;
            CurrentAmmo -= _unlimitedAmmo ? 0 : (uint)1;
            Reloading = false;
            if (SoundManager.Instance != null) SoundManager.Instance.PlayShoot();
            Vector2 firePoint = transform.position;

            for (int i = 0; i < _bulletCount; i++)
            {
                Vector2 bulletDirection = target - firePoint;

                // add spread to bullet direction
                bulletDirection = new Vector2(bulletDirection.x + Random.Range(-_bulletSpread, _bulletSpread), bulletDirection.y + Random.Range(-_bulletSpread, _bulletSpread));

                GameObject bulletInstance = Instantiate(_bulletPrefab, firePoint, Quaternion.identity);
                if (bulletInstance.TryGetComponent(out Bullet bullet))
                {
                    bullet.Fire(bulletDirection.normalized * _bulletSpeed, _knockbackMultiplier, _bulletDamage);
                    if (PlayerManager.Instance) bullet.IsEnemy = PlayerManager.Instance.Player != gameObject;
                }
            }
        }
    }
}