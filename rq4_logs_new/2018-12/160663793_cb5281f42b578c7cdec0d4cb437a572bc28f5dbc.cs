using UnityEngine;

namespace Weapons
{
	public class ProjectileWeapon : Weapon
	{
		[SerializeField] private GameObject _projectile;
		[SerializeField] private float _initialForce;

		protected override void SpawnBullet(Vector3 targetPoint)
		{
			// Spawn the new projectile.
			var newProjectile = Instantiate(_projectile, GunEnd.position, Quaternion.identity, null);
			
			// Point it towards the target object.
			newProjectile.transform.LookAt(targetPoint);
			newProjectile.GetComponent<Rigidbody>().AddForce(Vector3.forward * _initialForce);
		}
	}
}