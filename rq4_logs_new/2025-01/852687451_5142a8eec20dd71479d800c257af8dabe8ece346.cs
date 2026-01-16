using TMPro;
using UnityEngine;

[RequireComponent(typeof(TMP_Text))]
public class MoneyUI : MonoBehaviour
{
    [SerializeField] private TMP_Text _text;

    private PlayerStats _playerStats;
    private Shop _shop;

    #region Validate
    private void OnValidate()
    {
        _text = GetComponent<TMP_Text>();
    }
    #endregion 

    public void Inititialize(PlayerStats playerStats, Shop shop)
    {
        _playerStats = playerStats;
        _shop = shop;
        shop.CallingSave += UpdateText;
        UpdateText();
    }

    private void OnEnable()
    {
        _shop.CallingSave += UpdateText;
    }

    private void OnDisable()
    {
        _shop.CallingSave -= UpdateText;
    }

    private void UpdateText()
    {
        _text.text = _playerStats.Money.ToString();
    }
}