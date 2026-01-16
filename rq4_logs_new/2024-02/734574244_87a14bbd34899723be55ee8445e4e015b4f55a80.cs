using System;
using UnityEngine;

namespace Suhdo.Weapons.Components
{
	 public abstract class WeaponComponent : MonoBehaviour
	 {
		 protected Weapon weapon;
		 protected bool isAttackActive;

		 protected virtual void Awake()
		 {
			 weapon = GetComponent<Weapon>();
		 }

		 protected virtual void EnterHandle()
		 {
			 isAttackActive = true;
		 }

		 protected virtual void ExitHandle()
		 {
			 isAttackActive = false;
		 }

		 protected virtual void OnEnable()
		 {
			 weapon.OnEnter += EnterHandle;
			 weapon.OnExit += ExitHandle;
		 }

		 protected virtual void OnDisable()
		 {
			 weapon.OnEnter -= EnterHandle;
			 weapon.OnExit -= ExitHandle;
		 }
	 }
}