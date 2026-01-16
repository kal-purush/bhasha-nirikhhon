using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class New_GameOver_M : MonoBehaviour
{
    public Text gameOver;
    public Image blackBack;
    public GameObject over1, over2;
    private float timer1, timer2;
    private CriAtomSource cas;
    private bool off1, off2;

    void Start()
    {
        cas = GetComponent<CriAtomSource>();
    }

    // Update is called once per frame
    void Update()
    {
        timer1 += 0.05f;
        timer2 += 0.05f;

        gameOver.color = new Color(1, 1, 1, timer1);
        blackBack.color = new Color(0, 0, 0, timer1);

        if (timer1 >= 1.1f && !off1)
        {
            //からあげ絵が出てくる
            //レンチン音を鳴らすらしいので、設定お願いします
            //cas.Play("");
            over1.SetActive(true);
            off1 = true;
        }
        else if (timer2 >= 1.5f && !off2)
        {
            //ボタンが出てくる
            over2.SetActive(true);
            off2 = true;
            Cursor.visible = true;
        }

        if (off1)
        {
            timer1 = 1;
        }
        if (off2)
        {
            timer2 = 1;
        }
    }
}