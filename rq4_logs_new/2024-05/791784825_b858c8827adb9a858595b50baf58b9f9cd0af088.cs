using UnityEngine;
using UnityEngine.UI;

public class BossHealthBar : MonoBehaviour
{
    public Slider easehealthSlider;
    public Slider healthSlider;
    public EnemyStats playerStats;  // Reference to the PlayerStats component

    private float LerpSpeed = 5f;

    private void Start()
    {
        // Initialize the sliders
        healthSlider.maxValue = playerStats.maxHp;
        easehealthSlider.maxValue = playerStats.maxHp;
        healthSlider.value = playerStats.hp;
        easehealthSlider.value = playerStats.hp;
    }

    private void Update()
    {
        // Update the primary health slider with the current health from PlayerStats
        if (healthSlider.value != playerStats.hp)
        {
            healthSlider.value = playerStats.hp;
        }

        // Smoothly update the ease health slider to catch up to the primary slider
        if (easehealthSlider.value != healthSlider.value)
        {
            easehealthSlider.value = Mathf.Lerp(easehealthSlider.value, healthSlider.value, LerpSpeed * Time.deltaTime);
        }
    }
}