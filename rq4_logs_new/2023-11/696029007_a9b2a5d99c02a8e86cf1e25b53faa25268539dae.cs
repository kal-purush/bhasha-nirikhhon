using System.Collections;
using System.Collections.Generic;
using TMPro;
using UnityEngine;
using UnityEngine.UI;

public class LevelCompleteManager : MonoBehaviour
{
    public GameObject[] promptImageGameObject;
    public TextMeshProUGUI[] promptTexts;
    // Start is called before the first frame update
    void Start()
    {
        for (int i = 0; i < promptImageGameObject.Length; i++)
        {
            promptImageGameObject[i].GetComponent<Image>().sprite = GameManager.Instance.promptSprites[i];
            promptTexts[i].text = GameManager.Instance.Prompts[i].DescriptionText;
        }
    }
}