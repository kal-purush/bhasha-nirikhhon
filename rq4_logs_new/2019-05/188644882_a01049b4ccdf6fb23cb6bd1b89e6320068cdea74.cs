using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class PlayerController : MonoBehaviour
{
    Vector3 pos; //描画使用
    public float Y;//ジャンプと奥行き移動用変数

    public float Z;
    public float jumpchousei = 0;
    public float kuchujumpbousi = 0;

    public float speed = 0.05f;//横移動スピード
    public float speedz = 0.02f;//縦軸移動すぴーと

    public float speedy = 0.2f;//ジャンプスピード変数
    public float gravity = -0.15f;//重力常にかけるよう

    bool jumpstate = false;//ジャンプ状態用


    Animator animator;//アニメーションの使い方わからんので参考書のうつし
    
    // Start is called before the first frame update
    void Start()
    {
        this.animator = GetComponent<Animator>();//アニメーション系は参考書の写し
  



    }
    // Update is called once per frame
    void Update()
    {

        if (Y < 0) Y = 0;//Yの範囲制限
        if (Y > 5) Y= 5;

        if (Y > 0)//浮いたら重力かける＆ジャンプアニメーション
        {
            animator.Play("Jump");
            Y += gravity;
        }



        Vector3 scale = transform.localScale;
        if (Input.GetAxisRaw("Horizontal") > 0)
      
        {
            scale.x = 5; // そのまま（右向き）

            pos.x += speed;//speedを座標にぶっこむ
            animator.Play("Walk");//動いたときにアニメーション

        }
        else if (Input.GetAxisRaw("Horizontal") < 0)
        {
            scale.x = -5; // 反転する（左向き）

            pos.x += -speed;//speedをマイナスして座標にぶっこむ
            animator.Play("Walk");

        }
        transform.localScale = scale;

        if (Input.GetAxisRaw("Vertical") > 0)
        {
            Z += speedz;//speedzを座標にぶっこむ
            animator.Play("Walk");//うごいたときにアニメーション

        }
        if (Input.GetAxisRaw("Vertical") < 0)
        {
            Z += -speedz;//speedzマイナスして座標にぶっこむ
            animator.Play("Walk");
        }

       if(Input.GetKey(KeyCode.Space)&&jumpchousei==0&&kuchujumpbousi<0.3)//スペースおしたらジャンプ   ジャンプ時間調整変数追加
        {
            Y += speedy;
            kuchujumpbousi += Time.deltaTime;
            
        }
        if (Input.GetKeyUp(KeyCode.Space))//スペースはなしたら着地するまでジャンプできない　空中ジャンプ防止
        {
            jumpchousei += 1;
        }

        pos.y = Z + Y;
        transform.position = pos;
        if (Y == 0)//着地したらspeedyとjumpchouseiを初期値に戻す
        {
           
            speedy = 0.2f;
            jumpchousei = 0;
            kuchujumpbousi = 0;
        }

        if (Input.GetAxisRaw("Vertical") == 0&& Input.GetAxisRaw("Horizontal") == 0)//動いていないときはアニメーション停止
        {

            animator.Play("Stand");

        }



    }
}