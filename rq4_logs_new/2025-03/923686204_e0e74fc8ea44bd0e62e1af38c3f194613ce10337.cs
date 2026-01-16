using UnityEngine;
using UnityEngine.UI;
using TMPro;
using UnityEngine.EventSystems;

public class BuffItemUI : MonoBehaviour, IPointerEnterHandler, IPointerExitHandler
{
    [SerializeField] private Image buffIcon;
    [SerializeField] private TMP_Text stackText;
    [SerializeField] private Image background;

    private int stackCount = 1;
    private string buffDescription;
    private string buffName;
    public void Setup(BuffData buff)
    {
        buffIcon.sprite = buff.icon;
        buffName = buff.buffName;
        buffDescription = buff.description;
        stackCount = 1;
        UpdateStackUI();

        background.color = GetColorForRarity(buff.rarity);
    }

    public void IncrementStack()
    {
        stackCount++;
        UpdateStackUI();
    }

    private void UpdateStackUI()
    {
        stackText.text = stackCount.ToString();
    }

    private Color GetColorForRarity(RarityLevel rarity)
    {
        switch (rarity)
        {
            case RarityLevel.Common:
                return Color.white;
            case RarityLevel.Uncommon:
                return Color.green;
            case RarityLevel.Rare:
                return Color.blue;
            case RarityLevel.Epic:
                return Color.blue;
            case RarityLevel.Legendary:
                return Color.yellow;
            default:
                return Color.gray;
        }
    }

    public void OnPointerEnter(PointerEventData eventData)
    {
        Tooltip.Instance.ShowTooltip(buffName, buffDescription);
    }

    public void OnPointerExit(PointerEventData eventData)
    {
        Tooltip.Instance.HideTooltip();
    }
}