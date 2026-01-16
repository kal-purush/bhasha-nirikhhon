using UnityEngine;
using System.Collections;

public class SCORE : MonoBehaviour {
    //スコアの描画をするクラス
    public GameObject g_Score;
    private long score;

	// Use this for initialization
	void Start () {
        //スコアを初期値0に指定
        score = 0;
    }
	
	// Update is called once per frame
	void Update () {

        if (Input.GetKeyDown(KeyCode.C))
        {
            ScoreUp();
        }

        //スコア描画処理
        g_Score.GetComponent<TextMesh>().text = "score : " + score;

	}

    //スコアをアップする処理
    public long ScoreUp() {
        score+=1000;
        return score;
    }
    
}