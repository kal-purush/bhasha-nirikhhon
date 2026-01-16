using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ItemAssets : MonoBehaviour
{
   public static ItemAssets Instance { get; private set; }

   private void Awake()
   {
      Instance = this;
   }

   public Sprite keySprite;
   public Sprite ammoSprite;
   public Sprite keycardgreenSprite;
   public Sprite keycardredSprite;
   public Sprite keycardorangeSprite;
   public Sprite keycardblueSprite;
   public Sprite keycardpurpleSprite;
   public Sprite keycardyellowSprite;
     

}