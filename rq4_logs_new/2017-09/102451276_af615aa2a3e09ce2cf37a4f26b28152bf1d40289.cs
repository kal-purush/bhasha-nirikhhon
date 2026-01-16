using UnityEngine;

namespace Main.Scripts.BulletProperties
{
	public class NailGunBulletProperties : MonoBehaviour {

		public float Lifetime = 2.0f;           //Time untill bullets clear
		public int Damage = 1;
		int _health;

		void Start () {
			Destroy(gameObject, Lifetime);
		}
	
		void OnTriggerEnter(Collider other)
		{
			if (other.gameObject.CompareTag("Hitbox"))
			{
				other.gameObject.GetComponent<EntityHealth>().Health -= Damage;
				FindObjectOfType<AudioManager>().Play("codHit");
				Destroy(gameObject);
			}
			if (other.gameObject.CompareTag("Matter") || other.gameObject.CompareTag("Hazard"))
			{
				Destroy(gameObject);
			}
		}
	}
}