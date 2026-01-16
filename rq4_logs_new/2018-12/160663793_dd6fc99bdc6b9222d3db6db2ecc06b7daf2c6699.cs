using UnityEngine;

namespace Player
{
	public class PlayerHealth : MonoBehaviour
	{
		[SerializeField] private int _maxHealth;
		[SerializeField] private int _currentHealth;
		[SerializeField] private int _maxShield;
		[SerializeField] private int _currentShield;

		public void Heal(int amount)
		{
			// Figure out the amount to heal, to prevent over healing.
			var amountToHeal = Mathf.Clamp(amount, 0, _maxHealth - _currentHealth);
			_currentHealth += amountToHeal;
		}

		public void Hurt(int amount)
		{
			// First try to get rid of the shield.
		}
	}
}