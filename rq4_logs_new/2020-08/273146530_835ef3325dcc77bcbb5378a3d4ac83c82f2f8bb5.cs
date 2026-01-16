using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class TimeCounter : MonoBehaviour
{
    //カウントアップ
    private float countup = 0.0f;

    //時間を表示するText型の変数
    public Text timeText;
    
    //クリックされたかどうか
    private bool isClick = false;

    // Update is called once per frame
    void Update()
    {
        //クリックされたかどうか
        if(isClick)
        {
            //クリックされた後は以降の処理を実行しないためにreturn
            return;
        }
        
        //時間をカウントする
        countup += Time.deltaTime;

        //時間を表示する
        timeText.text = countup.ToString("f1");

        //クリックした瞬間
        if (Input.GetMouseButtonDown(0))
        {
            timeText.text = "時間は" + countup.ToString("f2") + "秒です";
            isClick = true;
        }
    }
}