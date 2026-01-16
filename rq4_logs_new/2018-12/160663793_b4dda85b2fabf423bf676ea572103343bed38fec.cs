using UnityEngine;

public class WeaponBase : MonoBehaviour
{
	//weapon name and image of weapon.
	[SerializeField] private string _name;
	[SerializeField] private Sprite _icon;
	
	//weapons stats.
	[Space]
	[SerializeField] private int _damage;
	[SerializeField] private int _headshotdamage;
	[SerializeField] private float _knockBack;
	[SerializeField] private float _range;
	[SerializeField] private int _ammo;
	[SerializeField] private AmmoType _ammoType;
	
	private enum AmmoType
	{
		Pistol, Rocket, Rifle, Shotgun, Sniper
	}
}