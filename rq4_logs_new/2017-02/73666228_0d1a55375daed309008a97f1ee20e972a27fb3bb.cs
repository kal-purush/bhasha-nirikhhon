using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ResultManager : MonoBehaviour {
    //白フェード！
    //フラグ受け取る！
    //勝ったほうにWIN、負けたらLOSE表示！
    //Display間違えないように！
    //モーションは仕様書参考！カメラグルグル！
    //タイムで遷移・・・の前にプレイありがとう画面表示！と同時にプレイ回数カウント！
    //黒フェードで遷移！

    private float player1HP;
    private float player2HP;

    private void createResultUI()
    {
        //1P Wins
        if(player1HP > player2HP)
        {
            //UIをそれぞれに表示。
            //カメラは同一動作でいいかも。
            //ObjectはLayerつけて見せるカメラ調整しようか
        }
        //2P Wins
        else if(player1HP < player2HP)
        {
        }
        //draw
        else
        {
        }
    }

    void Start ()
    {	
	}

	void Update ()
    {	
	}
}