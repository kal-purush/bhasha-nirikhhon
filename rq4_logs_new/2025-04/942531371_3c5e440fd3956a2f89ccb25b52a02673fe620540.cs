using System;
using UnityEngine;
using UnityEngine.UI;

public class UIShopSlot: UIBase
{
        [SerializeField] private int scrollAmount;
        [SerializeField] private int goldPrice;

        private Button buyButton;

        private void Awake()
        {
                buyButton = GetComponentInChildren<Button>();
        }

        private void Start()
        {
                buyButton.onClick.AddListener(() =>
                {
                        SocketManager.Instance.PushBuyData(scrollAmount,goldPrice);
                });

                UIMainMenuManager.Instance.OnResourceChange += ((int gold, int scroll) =>
                {
                        if (gold >= goldPrice)
                        {
                                EnableBuyButton();
                        }
                        else
                        {
                                DisableBuyButton();
                        }
                });
        }

        public void DisableBuyButton()
        {
                buyButton.interactable = false;
        }

        public void EnableBuyButton()
        {
                buyButton.interactable = true;
        }
}