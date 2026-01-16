using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class GameManagerUI : MonoBehaviour
{
    public GameObject Player1;
    public GameObject Player2;

    public Text Player1Text;
    public Text Player2Text;

    public int color;

    bool Player1Plays;
    // Start is called before the first frame update
    void Start()
    {
        Player1 = GameObject.Find("player1");
        Player2 = GameObject.Find("player2");
    }

    // Update is called once per frame
    void Update()
    {



    }


    void SwitchSide()
    {
        if (Player1Plays == true) Player1Plays = false;
        
        else
        Player1Plays = true;
    }

    void ShowColor()
    {
        switch (color)
        {
            case 1:
                // color red



                break;
            case 2:
                // color blue


                break;
            case 3:
                // color yellow



                break;
            case 4:
                // color green


                break;
            default:
                // code block


                break;
        }
    }




}