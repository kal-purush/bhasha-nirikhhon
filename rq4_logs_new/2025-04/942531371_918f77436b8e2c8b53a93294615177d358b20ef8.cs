using System;
using TMPro;
using UnityEngine;

public class UIPlayerResource: UIBase
{
        [SerializeField] private TextMeshProUGUI textGold;
        [SerializeField] private TextMeshProUGUI textScroll;

        // private int countCall;
        //
        // private void Awake()
        // {
        //         countCall = 0;
        // }
        //
        // private void OnEnable()
        // {
        //         if (countCall == 0)
        //         {
        //                 countCall++;
        //         }
        //         else
        //         {
        //                 SocketManager.Instance.ResquestResourceData();
        //         }
        // }

        public void SetResourceText(int gold, int scroll)
        {
                textGold.text = gold.ToString();
                textScroll.text = scroll.ToString();
        }
}