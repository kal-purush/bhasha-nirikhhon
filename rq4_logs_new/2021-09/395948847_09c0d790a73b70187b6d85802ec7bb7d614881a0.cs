using Photon.Pun;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

//新規作成(岩下 9/5)
public class AnswerTimer : MonoBehaviourPunCallbacks
{
    // Start is called before the first frame update

    public Text time_text;//残り時間

    void Start()
    {
        v.ans_time = 5.0f;//5秒
    }

    // Update is called once per frame
    void Update()
    {
        time_text.text = "残り" + v.ans_time.ToString("0") + "秒";
        if (!v.ans_time_flag)
        {
            v.ans_time -= Time.deltaTime;
            if (v.ans_time <= 0)
            {
                time_text.text = "読み込み中";
                v.turn_frag = true;
                v.ans_time_flag = true;
                Debug.Log(v.turn_frag);
            }
        }
    }
}