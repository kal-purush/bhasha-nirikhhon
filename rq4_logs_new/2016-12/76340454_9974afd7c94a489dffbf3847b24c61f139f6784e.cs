using UnityEngine;
using System.Collections;
using System.Collections.Generic;
using UnityEngine.SceneManagement;

public class GameController : Singleton<GameController>
{
   
    public GameObject player1;
    public GameObject player2;
    public Player player1Sc;
    public Player player2Sc;
    bool isPlayer1Clear;
    bool isPlayer2Clear;


    public void ToPlay()
    {

        if (GameManager.GameSceneProp == GameScene.TITLE)
        {
            GameManager.GameSceneProp = GameScene.PLAY;
            FadeManager.Instance.LoadScene("Main", 1);
        }
            
        if (GameManager.GameSceneProp == GameScene.PAUSE)
        {
            GameManager.GameSceneProp = GameScene.PLAY;
            Time.timeScale = 1.0f;

        }

    }


    public void ToGuide()
    {
        GameManager.GameSceneProp = GameScene.GUIDE;
    }

    public void ToTitle()
    {
        GameManager.GameSceneProp = GameScene.TITLE;
        FadeManager.Instance.LoadScene("Title", 1);
    }

           

    public void ToPause()
    {
        GameManager.GameSceneProp = GameScene.PAUSE;
        Time.timeScale = 0.0f;
    }

    public void ToRESULT1()
    {
        if (isPlayer1Clear)
        {
            GameManager.GameSceneProp = GameScene.RESULT1;
            FadeManager.Instance.LoadScene("RESULT1", 1);
        }
    }

    public void ToRESULT2()
    {
        if (isPlayer2Clear)
        {
            GameManager.GameSceneProp = GameScene.RESULT2;
            FadeManager.Instance.LoadScene("RESULT2", 1);
        }            
    }

    public void Retry()
    {
        GameManager.GameSceneProp = GameScene.PLAY;
        FadeManager.Instance.LoadScene("Main",2);       
    }

    public void Awake()
    {
        if(this != Instance)
        {
            Destroy(this);
            return;
        }

        DontDestroyOnLoad(this.gameObject);
    }

    public void ClearJudge()
    {
        if(player1Sc.pos <= 500 && player2Sc.pos >= 500)
        {
            isPlayer1Clear = true;
        }

        if (player2Sc.pos <= 500 && player1Sc.pos >= 500)
        {
            isPlayer2Clear = true;
        }

    }

    public static void CallStateSwitch()
    {
        GameManager gm = new GameManager();

        if (GameManager.pos == 120.01)
        {
            gm.StateSwitch(120.01f);
        }
        

        if (GameManager.pos == 240.01)
        {
            gm.StateSwitch(240.01f);
        }
       
    }

    
    // Use this for initialization
    void Start ()
    {
        player1Sc = player1.GetComponent<Player>();
        player2Sc = player2.GetComponent<Player>();
        isPlayer1Clear = false;
        isPlayer2Clear = false;
    }
	
	// Update is called once per frame
	void Update () {
	
	}

 
}