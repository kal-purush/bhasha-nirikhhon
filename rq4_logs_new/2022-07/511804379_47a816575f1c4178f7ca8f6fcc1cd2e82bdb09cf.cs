using TMPro;
using UnityEngine;
using UnityEngine.UI;

public class CharacterWidget : MonoBehaviour
{
    [SerializeField] private Button _button;
    [SerializeField] private GameObject _emptySlot;
    [SerializeField] private GameObject _infoCharacterSlot;
    [SerializeField] private TMP_Text _nameLabel;
    [SerializeField] private TMP_Text _levelLabel;
    [SerializeField] private TMP_Text _goldLabel;
    [SerializeField] private TMP_Text _hpLabel;
    [SerializeField] private TMP_Text _damageLabel;
    [SerializeField] private TMP_Text _experienceLabel;

    public Button Button => _button;

    public void ShowInfoCharacterSlot(string name, string level, string gold, string hp, string damage, string experience)
    {
        _nameLabel.text = $"Name:  {name}";
        _levelLabel.text = $"Level: {level}";
        _goldLabel.text = $"Gold: {gold}";
        _hpLabel.text = $"HP: {hp}";
        _damageLabel.text = $"Damage: {damage}";
        _experienceLabel.text = $"Experience: {experience}";
        
        _infoCharacterSlot.SetActive(true);
        _emptySlot.SetActive(false);
    }

    public void ShowEmptySlot()
    {
        _infoCharacterSlot.SetActive(false);
        _emptySlot.SetActive(true);
    }
}