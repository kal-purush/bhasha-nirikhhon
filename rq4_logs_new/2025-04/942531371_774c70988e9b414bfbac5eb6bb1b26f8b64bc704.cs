using System;
using TMPro;
using UnityEngine;

public class UIBattleEndNotification: UIBase
{
       [SerializeField] private TextMeshProUGUI battleStatusText;
       [SerializeField] private TextMeshProUGUI goldText;
       [SerializeField] private GameObject victoryIcon;
       [SerializeField] private GameObject loseIcon;

       public void SetVictory(int gold)
       {
              Show();
              victoryIcon.SetActive(true);
              loseIcon.SetActive(false);
              battleStatusText.text = "Victory";
              goldText.text = gold.ToString();
       }

       public void SetLose(int gold)
       {
              Show();
              victoryIcon.SetActive(false);
              loseIcon.SetActive(true);
              battleStatusText.text = "Defeat"; 
              goldText.text = gold.ToString();
       }
}