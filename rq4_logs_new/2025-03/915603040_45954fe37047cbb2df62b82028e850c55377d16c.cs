using System;
using TMPro;
using UnityEngine;
using UnityEngine.UI;

public class EnemyHealthBar : MonoBehaviour
{
    public Slider healthSlider;
    public TMP_Text healthBarText;

    Damageable enemyDamageable;

    private void Awake()
    {
        GameObject enemy = GameObject.FindGameObjectWithTag("GruzMother");

        if (enemy == null)
        {
            Debug.Log("No player found in the scene. Make sure it has tag 'Player'");
        }

        enemyDamageable = enemy.GetComponent<Damageable>();
    }
    void Start()
    {
        healthSlider.value = CalculateSliderPercentage(enemyDamageable.Health, enemyDamageable.MaxHealth);
        healthBarText.text = "HP " + enemyDamageable.Health + " / " + enemyDamageable.MaxHealth;
    }

    private void OnEnable()
    {
        enemyDamageable.healthChanged.AddListener(OnPlayerHealthChanged);
    }

    private void OnDisable()
    {
        enemyDamageable.healthChanged.RemoveListener(OnPlayerHealthChanged);
    }

    private float CalculateSliderPercentage(float currentHealth, float maxHealth)
    {
        return currentHealth / maxHealth;
    }

    private void OnPlayerHealthChanged(int newHealth, int maxHealth)
    {
        healthSlider.value = CalculateSliderPercentage(newHealth, maxHealth);
        healthBarText.text = "HP " + newHealth + " / " + maxHealth;
    }

}