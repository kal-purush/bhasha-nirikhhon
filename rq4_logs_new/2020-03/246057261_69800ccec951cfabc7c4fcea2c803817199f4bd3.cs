using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class PlayerUI : MonoBehaviour {

	public Entity player;

	private Text txtAmmo;
	private Text txtHealth;
	public Health playerHealth;

	public void Start () {
		txtAmmo = GameObject.Find("UI/AmmoCounter").GetComponent<Text>();
		txtHealth = GameObject.Find("UI/HealthCounter").GetComponent<Text>();
	}

	public void Ammo () {
		if (player.equipment != null) {
			txtAmmo.text = "Ammo: " + player.equipment.GetAmmoCount();
		}
		else {
			txtAmmo.text = "No weapon selected";
		}
	}

	public void Health () {
		txtHealth.text = "Health: " + playerHealth.entityHealth;
	}

	public void Update () {
		Ammo();
		Health();
	}
}