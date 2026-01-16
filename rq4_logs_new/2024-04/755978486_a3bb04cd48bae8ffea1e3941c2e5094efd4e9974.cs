using UnityEngine;
using UnityEngine.UI;
using Assets.Scripts.Combat;

public abstract class BaseHealthBar : MonoBehaviour
{
    protected GameObject _robot;
    protected Health _health;
    private Slider _slider;
    public Gradient gradient;
    public Image fill;

    protected abstract void HandlePlayerChanged(GameObject player);

    protected virtual void Awake()
    {
        _slider = GetComponent<Slider>();
    }

    protected virtual void Start()
    {
        PlayerManager.OnPlayerChanged += HandlePlayerChanged;
    }

    protected virtual void Update()
    {
        if (_health)
        {
            SetHealthBar();
        }
    }

    private void OnDisable()
    {
        PlayerManager.OnPlayerChanged -= HandlePlayerChanged;
    }

    public void SetHealthBar()
    {
        _slider.maxValue = _health.GetMaxHealth();
        _slider.value = _health.GetCurrentHealth();
        fill.color = gradient.Evaluate(_slider.normalizedValue);
    }

}