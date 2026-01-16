using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.SceneManagement;

public class PlayerController : MonoBehaviour {

    //宣言
    //Inspectorで調整するためのプロパティ
    //プレイヤーキャラの速度
    public float speed = 12.0f;
    //プレイヤーのジャンプパワー
    public float jumpPower = 500.0f;

   
    //内部で扱う変数

    //アニメ再生インデックス
    int animIndex;
    //ゴールチェック
    bool goalCheck;
    //接地チェック
    bool grounded;
    //ゴールタイム
    float goalTime;

    Rigidbody2D rigidbody;

    Animator animator;

	// Use this for initialization
	void Start () {
        //初期化
        grounded = false;
        animIndex = 0;
        goalCheck = false;

        rigidbody = GetComponent<Rigidbody2D>();
        animator = GetComponent<Animator>();
	}

    //プレイヤーキャラのコリジョンんい他のゲームオブジェクトのコリジョンが入った
    private void OnCollisionEnter2D(Collision2D collision)
    {
        //ゴールチェック
        if(collision.gameObject.name == "Stage_Gate")
        {
            //ゴール
            goalCheck = true;
            goalTime = Time.timeSinceLevelLoad;
        }

      
    }

    // フレームの書き換え
    void Update () {

        //地面チェック
        Transform groundCheck = transform.Find("GroundCheck");
        grounded = (Physics2D.OverlapPoint(groundCheck.position) != null) ? true : false;


        //接地チェック
        if (grounded)
        {
            //ジャンプ
            if (Input.GetButtonDown("Fire1"))
            {
                //ジャンプ処理
                rigidbody.AddForce(new Vector2(0.0f, jumpPower));
            }
            //走りアニメーションを設定
            animator.SetTrigger("Run");
        }
        else
        {
            //ジャンプアニメーションを設定
            animator.SetTrigger("Jump");
        }
        //穴に落ちたか
        if(transform.position.y < -10.0f)
        {
            //穴に落ちたらステージを再読み込みしてリセット
            // 現在のScene名を取得する
            Scene loadScene = SceneManager.GetActiveScene();
            // Sceneの読み直し
            SceneManager.LoadScene(loadScene.name);
        }
    }
    //フレームの書き換え
    private void FixedUpdate()
    {
        //移動計算
        rigidbody.velocity = new Vector2(speed, rigidbody.velocity.y);
        //カメラ移動
        GameObject goCam = GameObject.Find("Main Camera");
        goCam.transform.position = new Vector3(transform.position.x + 5.0f, goCam.transform.position.y, goCam.transform.position.x);
    }

    //UnityGUIの表示
    private void OnGUI()
    {
        //デバッグテキスト
        GUI.TextField(new Rect(10, 10, 300, 60), "[Unity 2D Sample 3-1 C]\nマウスの左ボタンを押すとジャンプ！");
        if (goalCheck)
        {
            GUI.TextField(new Rect(10, 100, 330, 60), string.Format("******Goal!!*****\nTime{0}", goalTime));
        }

        //リセットボタン
        if(GUI.Button(new Rect(10, 80, 100, 20), "リセット"))
        {
            // 現在のScene名を取得する
            Scene loadScene = SceneManager.GetActiveScene();
            // Sceneの読み直し
            SceneManager.LoadScene(loadScene.name);
        }
    }
}