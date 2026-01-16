using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ShowCombo : MonoBehaviour {

    public int pos = 0;
    public int percentofsizeheight = 25;

    public bool top;
    public bool bottom;


    public GameObject gameManager;
    private Vector3 Position = new Vector3(0.0f, 0.0f, 0.0f);//value of setting position

    private CombiManager combiManager = null;
    void Start()
    {
        combiManager = gameManager.GetComponent<CombiManager>();
        GetComponent<RectTransform>().sizeDelta = new Vector2((Screen.width/2) / combiManager.Size, (Screen.width / 2) / combiManager.Size);

        Position.x = Screen.width / combiManager.Size * pos - Screen.width / 2;
        Position.y = 0 - (Screen.width / 2) / combiManager.Size / 2;

        if (top)
        {
            Position.x = Screen.width / combiManager.Size * pos - Screen.width / 4;
            Position.y = Screen.height - Screen.height / 2;
        }

        if (bottom)
        {
            Position.x = Screen.width / combiManager.Size * pos - Screen.width / 2;
            Position.y = 0 - (Screen.width / combiManager.Size) / 2;
        }

        GetComponent<RectTransform>().localPosition = Position;
    }

    private void Update()
    {
        if (combiManager.GetCombination()[pos] == VSCombinationHandler.Button.RED)
        this.GetComponentInChildren<SpriteRenderer>().sprite = combiManager.red;
        if (combiManager.GetCombination()[pos] == VSCombinationHandler.Button.BLUE)
            this.GetComponentInChildren<SpriteRenderer>().sprite = combiManager.blue;
        if (combiManager.GetCombination()[pos] == VSCombinationHandler.Button.YELLOW)
            this.GetComponentInChildren<SpriteRenderer>().sprite = combiManager.yellow;
        if (combiManager.GetCombination()[pos] == VSCombinationHandler.Button.GREEN)
            this.GetComponentInChildren<SpriteRenderer>().sprite = combiManager.green;
    }
}