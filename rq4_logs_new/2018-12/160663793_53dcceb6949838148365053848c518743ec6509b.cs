using System.Collections.Generic;
using UnityEngine;

namespace Weapons
{
	public class Inventory : MonoBehaviour
	{
		[SerializeField] private GameObject _activeWeapon;
		[SerializeField] private GameObject _previousWeapon; 
		[SerializeField] private List<GameObject> _inventory = new List<GameObject>(5);
		[SerializeField] private bool _swapToPickup;

		enum WeaponType
		{
			Pistol, Rifle, Boopgun, Sniper, Shotgun 
		}

		public void SwitchWeapons(int index)
		{
			_previousWeapon = _activeWeapon;
			_activeWeapon = _inventory[index];
		}

		public void AddWeapon(int index)
		{
			_inventory[index].GetComponent<Weapon>().SetEnabled(true);
			
			if (_swapToPickup)
			{
				SwitchWeapons(index);
			}
		}

		public void RemoveWeapon(int index)
		{
			// Swap to the previous weapon if the current one is being removed.
			if (index == _inventory.IndexOf(_activeWeapon))
			{
				SwitchWeapons(_inventory.IndexOf(_previousWeapon));
			}
			_inventory[index].GetComponent<Weapon>().SetEnabled(false);
		}
	}
}