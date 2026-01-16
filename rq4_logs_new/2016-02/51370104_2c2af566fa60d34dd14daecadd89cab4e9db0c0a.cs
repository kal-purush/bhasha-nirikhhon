using UnityEngine;
using System.Collections;

public class HUD : MonoBehaviour
{
    public Texture2D crosshairSprite;
    public Texture2D plus80;
    public Texture2D plus10;
    public Texture2D plus;
    public Texture2D min100;
    public Rect minusPos;
    public Rect plusPos;
    public Rect crossPos;
    public Rect scorePos;
    public GUIStyle textstyle;
    public float scaleTrigger = 0;
    public float scaleDelay = 0;
    public float plusDelay = 0;
    public float plusStay = 0;
    public float plusUp = 0;
    public float minusDelay = 0;
    public float minusStay = 0;
    public float minusUp = 0;
    public static bool crosshairTrigger = true;
    public bool scaleToADS = false;
    public bool scaleFromADS = false;
    public bool playerHit = false;
    public bool plusTrigger = false;
    public bool minusTrigger = false;
    public static bool enemyHit = false;
    public static bool enemyDie = false;
    public static int score = 0;

    void Start()
    {
        crossPos = new Rect((Screen.width - crosshairSprite.width / 2) / 2, (Screen.height - crosshairSprite.height / 2) / 2, crosshairSprite.width / 2, crosshairSprite.height / 2);
        scorePos = new Rect((Screen.width - 200) / 24 * 23, (Screen.height - 100) / 12, 200, 100);
        plusPos = new Rect(scorePos.x + 90, scorePos.y - 3, plus80.width * 1.3f, plus80.height * 1.3f);
        minusPos = new Rect(scorePos.x + 90, scorePos.y + 3, plus80.width * 1.3f, plus80.height * 1.3f);
    }

    void OnGUI()
    {
        //Score GUI
        GUI.Label(scorePos, "Score: " + score, textstyle);
        if(plusTrigger)
        {
            GUI.DrawTexture(plusPos, plus);
        }
        if (minusTrigger)
        {
            GUI.DrawTexture(minusPos, min100);
        }

        //Score handling
        if (enemyHit)
        {
            plusPos = new Rect(scorePos.x + 130, scorePos.y - 3, plus80.width * 1.3f, plus80.height * 1.3f);
            plusDelay = 0;
            plusStay = 0;
            plusTrigger = true;
            plus = plus10;
            enemyHit = false;
        }
        else if (enemyDie)
        {
            plusPos = new Rect(scorePos.x + 130, scorePos.y - 3, plus80.width * 1.3f, plus80.height * 1.3f);
            plusDelay = 0;
            plusStay = 0;
            plusTrigger = true;
            plus = plus80;
            enemyDie = false;
        }
        else if (playerHit)
        {
            minusPos = new Rect(scorePos.x + 90, scorePos.y + 3, plus80.width * 1.3f, plus80.height * 1.3f);
            minusDelay = 0;
            minusStay = 0;
            minusTrigger = true;
            playerHit = false;
        }

        //Run/ADS/Idle controller
        if (!PlayerController.ADS && Input.GetMouseButtonDown(1) && !PlayerController.switchADS && !PlayerController.shooting)
        {
            scaleToADS = true;
        }
        else if (PlayerController.ADS && Input.GetMouseButtonDown(1) && !PlayerController.switchADS && !PlayerController.shooting)
        {
            scaleFromADS = true;
        }

        //Crosshair
        if (crosshairTrigger)
        {
            GUI.DrawTexture(crossPos, crosshairSprite);
        }
    }

    void Update()
    {
        //toADS
        if (scaleToADS && scaleDelay < 0.2 && !scaleFromADS)
        {
            scaleTrigger += 2.5f;
            scaleDelay += 1 * Time.deltaTime;
            crossPos = new Rect((Screen.width - crosshairSprite.width / 2 + scaleTrigger) / 2, (Screen.height - crosshairSprite.height / 2 + scaleTrigger) / 2, crosshairSprite.width / 2 - scaleTrigger, crosshairSprite.height / 2 - scaleTrigger);
        }
        if (scaleDelay >= 0.2 && scaleToADS && !scaleFromADS)
        {
            scaleToADS = false;
            scaleDelay = 0;
            crosshairTrigger = false;
            PlayerController.switchADS = false;
        }

        //fromADS
        if (scaleFromADS && scaleDelay < 0.2 && !scaleToADS)
        {
            scaleTrigger -= 2.5f;
            scaleDelay += 1 * Time.deltaTime;
            crosshairTrigger = true;
            crossPos = new Rect((Screen.width - crosshairSprite.width / 2 + scaleTrigger) / 2, (Screen.height - crosshairSprite.height / 2 + scaleTrigger) / 2, crosshairSprite.width / 2 - scaleTrigger, crosshairSprite.height / 2 - scaleTrigger);
        }
        if (scaleFromADS && !scaleToADS && scaleDelay >= 0.2)
        {
            scaleTrigger = 0;
            scaleFromADS = false;
            scaleDelay = 0;
            PlayerController.switchADS = false;
        }

        //Plus score
        if (plusTrigger && plusDelay < 0.5)
        {
            plusUp = 1.5f;
            plusDelay += 1 * Time.deltaTime;
            plusPos = new Rect(plusPos.x, plusPos.y - plusUp, plus80.width * 1.3f, plus80.height * 1.3f);
        }
        if (plusTrigger && plusDelay >= 0.5)
        {
            plusStay += 1 * Time.deltaTime;
        }
        if (plusStay >= 1.5)
        {
            plusDelay = 0;
            plusTrigger = false;
            plusStay = 0;
        }

        //Minus score
        if (minusTrigger && minusDelay < 0.5)
        {
            minusUp = 1.5f;
            minusDelay += 1 * Time.deltaTime;
            minusPos = new Rect(minusPos.x, minusPos.y + minusUp, plus80.width * 1.3f, plus80.height * 1.3f);
        }
        if (minusTrigger && minusDelay >= 0.5)
        {
            minusStay += 1 * Time.deltaTime;
        }
        if (minusStay >= 1.5)
        {
            minusDelay = 0;
            minusTrigger = false;
            minusStay = 0;
        }
    }
}