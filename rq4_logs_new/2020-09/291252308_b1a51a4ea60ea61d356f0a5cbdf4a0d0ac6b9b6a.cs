using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;
using TMPro;

public class HintButtonhandler : MonoBehaviour
{
    private Button hintButton;
    private GameManager gameManager;
    private DataController dataController;
    void Start()
    {
        hintButton = GetComponent<Button>();
        gameManager = GameObject.FindObjectOfType<GameManager>();
        dataController = FindObjectOfType<DataController>();
        Question question = gameManager.getCurrentQuestion();
        //PopUpSystem pop = this.GetComponent<PopUpSystem>();
        hintButton.onClick.AddListener(delegate {
            string hintText = gameManager.getCurrentQuestion().hint;
            //StartCoroutine("WaitForSec");
            PopUpSystem pop = this.GetComponent<PopUpSystem>();
            pop.popUp(hintText);
        });
    }

   /* IEnumerator WaitForSec()
    {
        yield return new WaitForSeconds(3);
        hintText.text = "";
    }*/
    void Destroy()
    {
        hintButton.onClick.RemoveAllListeners();
    }
}