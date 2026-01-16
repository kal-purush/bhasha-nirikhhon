using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;
using UnityEngine.SceneManagement;

public class ChoicePoint : MonoBehaviour {
    //属性
    private int currentChoice = 0;
    //外部引用
    public Image[] choose;
	
	// Update is called once per frame
	void Update () {
        if(Input.GetKeyDown(KeyCode.Return) && currentChoice == 0)
        {
            StartGame();
        }
        if (Input.GetKeyDown(KeyCode.Return) && currentChoice == 1)
        {
            ExitGame();
        }

        Choosing();
	}

    private void Choosing()
    {
        if(Input.GetKeyDown(KeyCode.UpArrow))
        {
            if(currentChoice > 0)
            {
                currentChoice--;
            }
            transform.position = choose[currentChoice].transform.position;
        }
        else if(Input.GetKeyDown(KeyCode.DownArrow))
        {
            if(currentChoice < 2)
            {
                currentChoice++;
            }
            transform.position = choose[currentChoice].transform.position;
        }
    }

    private void StartGame()
    {
        SceneManager.LoadScene(1);
    }

    private void ExitGame()
    {
        Application.Quit();
    }
}