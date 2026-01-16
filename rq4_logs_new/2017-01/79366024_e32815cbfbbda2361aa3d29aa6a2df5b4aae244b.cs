using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;
using UnityEngine;

public class LoadSaveGame : MonoBehaviour
{

    public static LoadSaveGame loadSaveGame;

    private void Start()
    {
        PlayerPrefs.SetInt("load", 0);
    }

    private void Awake()
    {
        if (loadSaveGame == null)
        {
            DontDestroyOnLoad(gameObject);
            loadSaveGame = this;
        }
        else if (loadSaveGame != this)
        {
            Destroy(loadSaveGame);
        }
    }

    public void Save()
    {

        BallBehavior BallOfEvil = GameObject.Find("Ball of Evil").GetComponent<BallBehavior>();
        PaddleAI aiPaddle = GameObject.Find("Paddle - CPU").GetComponent<PaddleAI>();
        GameLogic gameLogic = GameObject.Find("Main Camera").GetComponent<GameLogic>();

        BinaryFormatter bf = new BinaryFormatter();
        FileStream file = File.Create(Application.persistentDataPath + "/gameinfo.dat");

        GameInformation gi = new GameInformation();

        gi.aiPaddlex = aiPaddle.transform.position.x;
        gi.aiPaddley = aiPaddle.transform.position.y;
        gi.aiPaddlez = aiPaddle.transform.position.z;
        gi.ballLocationx = BallOfEvil.transform.position.x;
        gi.ballLocationy = BallOfEvil.transform.position.y;
        gi.ballLocationz = BallOfEvil.transform.position.z;
        gi.ballSpeed = BallOfEvil.velocity;
        gi.cpuScore = gameLogic.scoreCPU;
        gi.hmnScore = gameLogic.scoreHMN;

        bf.Serialize(file, gi);
        file.Close();
    }

    public void Load()
    {
        if (File.Exists(Application.persistentDataPath + "/gameinfo.dat"))
        {
            BallBehavior BallOfEvil = GameObject.Find("Ball of Evil").GetComponent<BallBehavior>();
            PaddleAI aiPaddle = GameObject.Find("Paddle - CPU").GetComponent<PaddleAI>();
            GameLogic gameLogic = GameObject.Find("Main Camera").GetComponent<GameLogic>();

            BinaryFormatter bf = new BinaryFormatter();
            FileStream file = File.Open(Application.persistentDataPath + "/gameinfo.dat", FileMode.Open);
            GameInformation gi = (GameInformation)bf.Deserialize(file);
            file.Close();


            aiPaddle.transform.position = new Vector3(gi.aiPaddlex, gi.aiPaddley, gi.aiPaddlez);
            BallOfEvil.transform.position = new Vector3(gi.ballLocationx, gi.ballLocationy, gi.ballLocationz);
            BallOfEvil.velocity = gi.ballSpeed;
            gameLogic.scoreHMN = gi.hmnScore;
            gameLogic.scoreCPU = gi.cpuScore;
        }
    }
}

[Serializable]
class GameInformation {

    public float aiPaddlex;
    public float aiPaddley;
    public float aiPaddlez;

    public float ballLocationx;
    public float ballLocationy;
    public float ballLocationz;

    public float ballSpeed;

    public int hmnScore;

    public int cpuScore;
}