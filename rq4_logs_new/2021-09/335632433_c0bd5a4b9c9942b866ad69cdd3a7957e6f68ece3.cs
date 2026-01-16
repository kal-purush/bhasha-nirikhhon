using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class ScoreTime : MonoBehaviour
{
    float time;
    float timeD;
    public GameObject boss;
    public GameObject bossParent;
    public Text timeText;
    [SerializeField]
    string str;
    [SerializeField]
    int cutMove;
    [SerializeField]
    int cutStop;

    // Start is called before the first frame update
    void Start()
    {
        time = 0.0f;
        timeD = -0.1f;
        if (str == "")
        {
            str = "Time";
        }
    }

    // Update is called once per frame
    void Update()
    {
        if(bossParent.activeInHierarchy == false)
        {
            time += Time.deltaTime;
        }
        else if(boss.activeInHierarchy == true)
        {
            time += Time.deltaTime;
        }

        if (timeD == time)
        {
            timeText.text = str + "：" + time.ToString($"f{cutStop}");
        }
        else
        {
            timeText.text = str + "：" + time.ToString($"f{cutMove}");
        }

        timeD = time;
    }
}