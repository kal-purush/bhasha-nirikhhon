using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class startmenuScript : MonoBehaviour
{
    [Tooltip("The Game Object that you would like to set inactive")]
    [SerializeField]
    private GameObject StartMenu;
    CardController cController;
    // Start is called before the first frame update
    void Start()
    {
        cController = GameObject.Find("SceneManager").GetComponent<CardController>();
    }

    public void closeMenu()
    {
        cController.startGame();
        StartMenu.SetActive(false);
    }
}