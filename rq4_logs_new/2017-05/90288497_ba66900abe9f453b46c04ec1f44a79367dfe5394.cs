using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.SceneManagement;

public class GameOverScript : MonoBehaviour {

    public int   playerHP;
    public float score;        //Pontuacao final do jogador quando ele morrer
    public float minimalScore; //Pontuacao que o jogador deve ter para não perder o jogo
    public float nextLevelScore; //Pontuacao que o jogador deve ter para passar de nível

    public string[] stageNames; //nome dos arquivos de cena de cada fase
    public int currentStage;

	// Use this for initialization
	void Start () {
		
	}
	
	// Update is called once per frame
	void Update () {

        if (playerHP <= 0)
        {
            EndCombat();
        }
		
	}

    void EndCombat()
    {

        if(score <= minimalScore)
        {
            SceneManager.LoadScene("GameOver");
        }

        else if (score < nextLevelScore)
        {
            SceneManager.LoadScene(stageNames[currentStage]);
        }

        else if (score >= nextLevelScore)
        {
            currentStage++;
            SceneManager.LoadScene(stageNames[currentStage]);
        }


    }
}