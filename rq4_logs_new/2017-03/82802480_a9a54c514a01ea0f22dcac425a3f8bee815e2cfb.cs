using UnityEngine;

public class AnimalBase : MonoBehaviour, IEntityLiving {
	
	[SerializeField]
	private GameObject deadAnimal;
	[SerializeField]
	private float health;

	public void TakeDamage(float amount) {
		health -= amount;
		if(health <= 0)
			Die(); 
	}
	
	public float GetHealth() {
		return health;
	}
	
	public void SetHealth(float amount) {
		health = amount;
	}
	
	private void Die() {
		Instantiate(deadAnimal,gameObject.transform.position, gameObject.transform.rotation);
		Destroy(gameObject);
	}
}