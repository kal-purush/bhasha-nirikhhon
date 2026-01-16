using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;
using UnityEngine.SceneManagement;
using UnityEngine.EventSystems;
using TMPro;
using UnityEngine.SceneManagement;

public class UiManager : MonoBehaviour
{
    public GameObject[] snArray = new GameObject[3];
    public Sprite lockSprite;
    public Sprite unLockSprite;

    public void Start()
    {
        Initialized();
    }

    public void Update()
    {
        // 1-1 ���������� �ر�
        if (Input.GetKeyDown(KeyCode.K))
        {
            for(int Sn = 1; Sn <= GameManager.maxScenario; Sn++)
            {
                for(int St = 1; St <= GameManager.maxStaage; St++)
                {
                    if (Sn == 1 && St == 1) PlayerPrefs.SetInt("" + Sn + "-" + St, 1);
                    else PlayerPrefs.SetInt("" + Sn + "-" + St, 0);
                }
            }
            Initialized();
        }

        // ��� �������� �ر�
        if (Input.GetKeyDown(KeyCode.L))
        {
            for (int Sn = 1; Sn <= GameManager.maxScenario; Sn++)
            {
                for (int St = 1; St <= GameManager.maxStaage; St++)
                {
                    PlayerPrefs.SetInt("" + Sn + "-" + St, 1);
                }
            }
            Initialized();
        }
    }

    private void Initialized()
    {
        if (!PlayerPrefs.HasKey("1-1"))
        {
            for (int Sn = 1; Sn <= GameManager.maxScenario; Sn++)
            {
                for (int St = 1; St <= GameManager.maxStaage; St++)
                {
                    if (Sn == 1 && St == 1) PlayerPrefs.SetInt("" + Sn + "-" + St, 1);
                    else PlayerPrefs.SetInt("" + Sn + "-" + St, 0);
                }
            }
        }

        for (int Sn = 1; Sn <= GameManager.maxScenario; Sn++)
        {
            for (int St = 1; St <= GameManager.maxStaage; St++)
            {
                if (PlayerPrefs.GetInt("" + Sn + "-" + St) == 1)
                {
                    snArray[Sn - 1].transform.GetChild(St - 1).GetComponent<Image>().sprite = unLockSprite;
                }
                else if (PlayerPrefs.GetInt("" + Sn + "-" + St) == 0)
                {
                    snArray[Sn - 1].transform.GetChild(St - 1).GetComponent<Image>().sprite = lockSprite;
                }
            }
        }
    }

    public void STButtonClick()
    {
        PlayerPrefs.DeleteKey("SN");
        PlayerPrefs.DeleteKey("ST");
        GameObject currentStObject = EventSystem.current.currentSelectedGameObject;
        if(PlayerPrefs.GetInt(currentStObject.transform.GetChild(0).GetComponent<TMP_Text>().text) == 1)
        {
            string[] selectStage = currentStObject.transform.GetChild(0).GetComponent<TMP_Text>().text.Split("-");
            PlayerPrefs.SetInt("SN", int.Parse(selectStage[0]));
            PlayerPrefs.SetInt("ST", int.Parse(selectStage[1]));

            SceneManager.LoadScene(2);
        }
    }

    public void SNButtonClick(GameObject stBox)
    {
        GameObject currentStObject = EventSystem.current.currentSelectedGameObject.transform.parent.gameObject;
        if (!stBox.activeSelf)
        {
            stBox.SetActive(true);
            currentStObject.transform.parent.GetChild(stBox.transform.GetSiblingIndex() + 1).gameObject.SetActive(false);
        }

        else
        {
            stBox.SetActive(false);
            currentStObject.transform.parent.GetChild(stBox.transform.GetSiblingIndex() + 1).gameObject.SetActive(true);
        }
    }

    public void BackButtonClick()
    {   
        SceneManager.LoadScene(0);
    }
}