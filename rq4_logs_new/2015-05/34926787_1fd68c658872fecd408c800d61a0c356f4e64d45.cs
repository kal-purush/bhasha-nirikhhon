using UnityEngine;
using UnityEngine.UI;
using System.Collections;

public class HealthSystem : MonoBehaviour {

	public static float MAX_HEALTH = 2000.0f;
	public Text healthDisplay;
	
	private float _currentHealth;

	// Use this for initialization
	public void Start () {
		_currentHealth = MAX_HEALTH;	
	}

	public void Update() {
		healthDisplay.text = "HP: " + _currentHealth;
	}

	public void InflictDamage(float amount)
	{
		_currentHealth -= amount;
	}

	public bool IsDead(){
		return _currentHealth <= 0.0f;
	}
}