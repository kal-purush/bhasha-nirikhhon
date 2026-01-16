using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class PartyScript : MonoBehaviour
{
    [SerializeField] private Image[] board = new Image[25];
    private Player player;
    // Start is called before the first frame update
    void Start()
    {
        Sprite card;
        card = (Sprite) Resources.Load("Assets/Images/Cards/1.jpg");
        board[0].GetComponent<Image>().sprite = card;

    }

    // Update is called once per frame
    void Update()
    {
        
    }
    
    /*
    public void generateBoard()
    {
        for (int i = 0; i < 5; i++)
        {
            for (int j = 0; j < 5; j++)
            {
                board[i, j].graphic.mainTexture
            }
        }
    }
    */
    
}