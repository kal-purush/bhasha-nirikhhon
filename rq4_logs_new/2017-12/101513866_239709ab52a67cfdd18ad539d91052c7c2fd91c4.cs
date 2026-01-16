using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class InfoPopup : MonoBehaviour
{
    public GameObject popup;
    public GameManager.ShopItems thisItem;
    public Text title;
    public Text description;
    float origionalXScale = 0.01720726f;

    private void OnMouseEnter()
    {
        switch (thisItem)
        {
            case GameManager.ShopItems.BASIC:
                title.text = "<Color=green>Basic Plant</Color> $25";
                description.text = "This is a basic plant that does a moderate amount of damage and is relatively slow firing.";
                break;
            case GameManager.ShopItems.FIRE:
                title.text = "<Color=red>Fire Plant</Color> $50";
                description.text = "This plant causes damage over time to bugs, meaning the longer the enemies are on screen the more damage they take (even if this plant has stopped shooting at it).";
                break;
            case GameManager.ShopItems.ICE:
                title.text = "<Color=blue>Ice Plant</Color> $50";
                description.text = "This plant causes bugs to slow down. It does a low amount of damage and has an average attack speed.";
                break;
            case GameManager.ShopItems.VOID:
                title.text = "<Color=purple>Void Plant</Color> $50";
                description.text = "This plant does an average amount of damage but shoots twice as fast as most other plants.";
                break;
            case GameManager.ShopItems.SPRINKLER:
                title.text = "<Color=yellow>Sprinkler</Color> $25";
                description.text = "This can be placed on a farm tile and makes it so you no longer have to water it!";
                break;
            case GameManager.ShopItems.FERTILIZER:
                title.text = "<Color=yellow>Fertilizer</Color> $50";
                description.text = "This can be placed on a farm tile and makes your plants grow twice as fast.";
                break;
        }

        popup.SetActive(true);
    }

    private void OnMouseExit()
    {
        popup.SetActive(false);
    }
}