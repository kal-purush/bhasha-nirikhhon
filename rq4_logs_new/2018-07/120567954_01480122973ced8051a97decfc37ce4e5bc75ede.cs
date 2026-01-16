using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.SceneManagement;

/// <summary>
/// House script
/// 1. Hint
/// 2. Next stage Trigger
/// </summary>
/// <author> SangJun, YeHun </author>
public class House : MonoBehaviour {

    private GameObject _hintCylinder;    // 힌트를 위한 빛기둥
    private float _hintTimer = 7.0f; // 힌트 지속시간
    private int _stageNum = 0;   // 스테이지 Number
    private int _hintCount = 0;  // 힌트 개수는 최대 3개
    private bool _hintTrigger = false;
    private bool _hintOnOff = false;

    private float _timer = 0.0f;

    private void Awake()
    {
        _stageNum = GameManager.instance.stageNum;
        _hintCylinder = GameObject.Find("HintCylinder");
        _hintCylinder.SetActive(_hintTrigger);
    }

    private void Update()
    {
        if (_hintOnOff && (_timer <= _hintTimer))
        {
            // 힌트 최소 지속시간 3초 확인
            if ((_timer <= _hintTimer) && (3.0f <= _hintTimer))
            {
                _timer += Time.deltaTime;

                if (_hintTrigger == false)
                {
                    _hintTrigger = !_hintTrigger;
                    _hintCylinder.SetActive(_hintTrigger);
                }
            }
            else
            {
                Debug.Log("더 이상 힌트를 사용할 수 없습니다.");
            }
        }
        else if (_hintOnOff && _timer >= _hintTimer)
        {
            _timer = 0.0f;
            _hintOnOff = !_hintOnOff;
            _hintTrigger = !_hintTrigger;
            _hintCylinder.SetActive(_hintTrigger);
        }
    }

    /// <summary>
    /// Use Hint.
    /// </summary>
    public void UseHint()
    {
        _hintOnOff = !_hintOnOff;

        _hintTimer -= 1.0f;

        ++_hintCount;
    }

    void OnTriggerStay(Collider col)
    {
        if (col.gameObject.tag == "Player")
        {
            if (Input.GetKeyDown(KeyCode.F))
            {
                // 마지막 스테이지인지 check
                if(_stageNum == 3)
                {
                    SceneManager.LoadScene("GameClearScene");

                    Debug.Log("GameClear");
                }
                else
                {
                    string temp = "Stage" + (_stageNum + 1);
                    
                    SceneManager.UnloadSceneAsync(_stageNum);
                    SceneManager.LoadScene(temp, LoadSceneMode.Additive);

                    Debug.Log(temp + "Load 완료");
                }
            }
        }
    }

}