using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class BurgerGenerator : MonoBehaviour {

   public enum fillings { BOTTOM_BUN, TOP_BUN, PATTY, LETTUCE, TOMATO, CHEESE, KETCHUP, MUSTARD };

   private fillings[] burger;

   public Text UIText;

   public string seed;

   // the number of elements in a burger
   [Range (3, 15)]
   public int numElements;

   // These will be used for random filling generation

	// Use this for initialization
	void Start () {
      generateBurger();

      UIText.text = "Build this burger:" + buildBurgText();
   }


   void generateBurger()
   {
      burger = new fillings[numElements];
      System.Random pseudoRand = new System.Random(seed.GetHashCode());

      for (int i = 0; i < numElements; i++)
      {
         // if i is the first element, add a bottom bun
         if (i == 0)
         {
            burger[i] = fillings.BOTTOM_BUN;
         }

         // if it is the last, add the top bun
         else if (i == numElements - 1)
         {
            burger[i] = fillings.TOP_BUN;
         }

         // else choose randomly
         else
         {
            int nextRand = pseudoRand.Next(0, 100);

            if (nextRand < 40)
            {
               burger[i] = fillings.PATTY;
            }
            else if (nextRand < 52)
            {
               burger[i] = fillings.LETTUCE;
            }
            else if (nextRand < 64)
            {
               burger[i] = fillings.TOMATO;
            }
            else if (nextRand < 76)
            {
               burger[i] = fillings.CHEESE;
            }
            else if (nextRand < 88)
            {
               burger[i] = fillings.KETCHUP;
            }
            else
            {
               burger[i] = fillings.MUSTARD;
            }
         }
      }
   }

   string buildBurgText()
   {
      string built = "";

      for (int i = 0; i < burger.Length; i++)
      {
         built += "\n";
         switch (burger[i])
         {
            case fillings.BOTTOM_BUN:
               built += "- Bottom bun";
               break;
            case fillings.TOP_BUN:
               built += "- Top bun";
               break;
            case fillings.PATTY:
               built += "- Patty";
               break;
            case fillings.LETTUCE:
               built += "- Lettuce";
               break;
            case fillings.TOMATO:
               built += "- Tomato";
               break;
            case fillings.CHEESE:
               built += "- Cheese";
               break;
            case fillings.KETCHUP:
               built += "- Ketchup";
               break;
            case fillings.MUSTARD:
               built += "- Mustard";
               break;
         }
      }

      return built;
   }
}