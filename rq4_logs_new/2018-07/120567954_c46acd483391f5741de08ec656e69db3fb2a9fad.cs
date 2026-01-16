using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.SceneManagement;

public class NextStageTrigger : MonoBehaviour {

    private int stageNum = 0;
    private int HintCount = 0;
    private bool HintTrigger = false;
    private float HintTimer = 7.0f; // 힌트 개수는 최대 3개
    private bool HintOnOff = false;
    GameObject HintCylinder;
    float Timer = 0.0f;

    // Use this for initialization
    void Start()
    {
        stageNum = GameManager.instance.stageNum;
        HintCylinder = GameObject.Find("HintCylinder");
        HintCylinder.SetActive(HintTrigger);
    }

    private void Update()
    {
        //Debug.Log(HintCylinder.ToString());

        if (HintOnOff && Timer <= HintTimer)
        {
            if (Timer <= HintTimer && 3.0f <= HintTimer) // 힌트 지속시간은 최소 3초
            {
                Timer += Time.deltaTime;
                if (HintTrigger == false)
                {
                    HintTrigger = !HintTrigger;
                    HintCylinder.SetActive(HintTrigger);
                }
            }
            else
            {
                Debug.Log("더 이상 힌트를 사용할 수 없습니다.");
            }
        }
        else if (HintOnOff && Timer >= HintTimer)
        {
            Timer = 0.0f;
            HintOnOff = !HintOnOff;
            HintTrigger = !HintTrigger;
            HintCylinder.SetActive(HintTrigger);
        }
    }

    public void UseHint()
    {
        HintOnOff = !HintOnOff;
        //++HintCount;
        HintTimer -= 1.0f;
    }

    void OnTriggerStay(Collider col)
    {
        if (col.gameObject.tag == "Player")
        {
            if (Input.GetKeyDown(KeyCode.F))
            {
                if(stageNum == 3)
                {
                    SceneManager.LoadScene("GameClearScene");
                }
                else
                {
                    string temp = "Stage" + (stageNum + 1);
                    Debug.Log(stageNum + "언로드할겁니다");
                    SceneManager.UnloadSceneAsync(stageNum);
                    SceneManager.LoadScene(temp, LoadSceneMode.Additive);
                }
            }
        }
    }

}