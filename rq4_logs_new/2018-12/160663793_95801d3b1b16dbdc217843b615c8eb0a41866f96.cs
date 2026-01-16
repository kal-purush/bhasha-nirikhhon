using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using Weapons;

namespace Weapons
{

	public class _BoopGun : MonoBehaviour
	{

		[SerializeField] private GameObject _bullet;
		[SerializeField] private UnityEngine.Camera _fpsCam;
		[SerializeField] private Transform _startPosition;
		[SerializeField] private float _range;

		// Unity Methods
		private void Update()
		{
			if (Input.GetKeyDown(KeyCode.Mouse0))
			{
				Shoot(_bullet);
			}
		}

		// Custom Methods.
		private void Shoot(GameObject bullet)
		{
			RaycastHit hit;
			if (Physics.Raycast(_fpsCam.transform.position, _fpsCam.transform.forward, out hit, _range))
			{
				Debug.Log(hit.transform.name);
				var bulletClone = Instantiate(bullet, _startPosition.position, Quaternion.identity);
				bulletClone.transform.LookAt(hit.point);
				var forceToAdd = Vector3.forward * 999999999999999;
				bullet.GetComponent<Rigidbody>().AddForce(forceToAdd);
			}
		}
	}
}