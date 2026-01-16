using UnityEngine;

public class Health : MonoBehaviour
{
    [SerializeField] private float startingHealth;
    public float currentHealth { get; private set; }
    private bool dead;
    [SerializeField] private LogicScript logic;

    private void Awake()
    {
        currentHealth = startingHealth;
    }

    public void TakeDamage(float _damage)
    {
        currentHealth = Mathf.Clamp(currentHealth - _damage, 0, startingHealth);

        if (currentHealth > 0)
        {
            // player still alive
            // todo: can add a taking damage animation
        }
        else
        {
            if (!dead)
            {
                // player dead
                // todo: can add a dying animation
                GetComponent<PlayerMovement>().enabled = false;
                logic.gameOver();
                dead = true;
            }
        }
    }
}