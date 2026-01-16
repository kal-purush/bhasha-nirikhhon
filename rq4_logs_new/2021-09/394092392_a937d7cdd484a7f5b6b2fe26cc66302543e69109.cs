
using System;
using Cameras;
using UnityEngine;

[Serializable]
public class GamePlayer
{
    private CameraManager cameraManager;
    private GameManager gameManager;
    public GameCamera cameraObj;
    public MouseLook camLook;
    public GameObject cursorObj;
    public PlayerHand playerHand;
    public GameObject bodyObj;
    public int playerLayer;
    public int cardsToDraw;
    public MouseHole mouseHole;
    public FadeUI playerPlanningText;

    [Header("Alcohol Meter")]
    public int totalAlcolol;
    public int currentAlcolol;
    public AlcololMeter alcololMeter;

    private bool init;

    void Start()
    {
        Init();
    }

    void Init()
    {
        if (init)
            return;
        
        gameManager = GameManager.Instance;
        cameraManager =  GameObject.FindObjectOfType<CameraManager>();

        init = true;
    }
    
    public void ActivatePlayer()
    {
        Init();
        
        cameraManager.Set(cameraObj);
        cursorObj.SetActive(true);
        bodyObj.SetActive(false);
        
        //set player hand player ref to me!
        playerHand.myPlayer = this;

        //draw 3 cards on the first turn...
        if (GameManager.Instance.phaseCounter == 1)
        {
            //set cards to draw
            cardsToDraw = gameManager.drawAtStart;
            
            //mousehole placement
            mouseHole.BeginPlacement(this);
        }
        else
        {
            //set cards to draw
            cardsToDraw = gameManager.drawAmount;
        }
        
        //player text
        playerPlanningText.FadeIn();
        
        //set alcolol meter
        totalAlcolol++;
        currentAlcolol = totalAlcolol;
        alcololMeter.gameObject.SetActive(true);
        alcololMeter.SetAlcololAmount(currentAlcolol);

        //set camera layer mask 
        gameManager.mainCam.cullingMask = gameManager.baseCullingMask | (1 << playerLayer);
    }

    public void ReduceAlcolol(int amount)
    {
        currentAlcolol -= amount;
        alcololMeter.SetAlcololAmount(currentAlcolol);
    }

    public void DeactivatePlayer()
    {
        cursorObj.SetActive(false);
        bodyObj.SetActive(true);
        alcololMeter.gameObject.SetActive(false);
    }
}