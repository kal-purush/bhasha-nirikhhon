using System.Collections;
using System.Collections.Generic;
using TMPro;
using UnityEngine;
using UnityEngine.UI;

/// <summary>
/// UI version of Creature Card Item.
/// Generally used for Announcements and visibility for player only. 
/// </summary>
public class CreatureCardUIItem : AudioHandler
{
    private Camera mainCam;
    private CreatureCard myCardData;
    
    [Header("Creature Card Settings")]
   [SerializeField] 
   private Image cardImage;
   [SerializeField] 
   private Image playerImage;
   [SerializeField] 
   private TMP_Text cardName;

   void Start()
   {
       mainCam = Camera.main;
   }
   
   public void InjectCreatureWithData(CreatureCard cardData, GamePlayer player)
   {
       myCardData = cardData;
       
       if (cardImage)
       {
           cardImage.sprite = cardData.cardSprite;
       }

       if (playerImage)
       {
           playerImage.sprite = player.playerSprite;
       }

       if (cardName)
       {
           cardName.text = cardData.cardName;
           gameObject.name = cardName + " Card";
       }
   }
}