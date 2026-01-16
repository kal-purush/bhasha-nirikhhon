using UnityEngine;
using UnityEngine.UI;

using System;
using System.Collections;

public class BuyPanel : MonoBehaviour
{
    [SerializeField] private GameObject panel;
    [SerializeField] private Image itemImage;

    Action confirmed;

    public void PopUp(Sprite item, Action confirm)
    {
        itemImage.sprite = item;

        confirmed = confirm;

        panel.SetActive(true);
    }

    public void PopDown()
    {
        panel.SetActive(false);
    }

    public void ConfirmAction()
    {
        if(confirmed != null)
            confirmed();
        PopDown();
    }
}