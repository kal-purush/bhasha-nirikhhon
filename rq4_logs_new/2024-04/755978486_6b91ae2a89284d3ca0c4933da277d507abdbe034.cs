using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;
using Assets.Scripts.Combat;

public class EnemyHealthBar : MonoBehaviour
{
    [SerializeField] private GameObject _gameObject;
    private Slider _slider;
    public Gradient gradient;
    public Image fill;

    private Health _health;

    void Awake()
    {
        _health = _gameObject.GetComponent<Health>();
        _slider = GetComponent<Slider>();
    }

    private void Update()
    {
        if (_health)
        {
            SetHealthBar();
        }
        gameObject.SetActive(_gameObject != PlayerManager.Instance.Player);
    }

    public void SetHealthBar()
    {
        _slider.maxValue = _health.GetMaxHealth();
        _slider.value = _health.GetCurrentHealth();
        fill.color = gradient.Evaluate(_slider.normalizedValue);
    }
}