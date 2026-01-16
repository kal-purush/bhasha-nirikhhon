using System.Collections;
using System.Collections.Generic;
using UnityEngine;

/// <summary>
/// Rigidbodyの速度を操作して移動するクラス
/// </summary>
[RequireComponent(typeof(Rigidbody))]//Rigidbody必須(Add時になければ自動で追加される)

public class PlayerMove : MonoBehaviour {
    [SerializeField]
    private Rigidbody _rigidbody = null;

    //VR上で目の中心(見ている方向)を確認する用のアンカー
    [SerializeField]
    private Transform _centerEyeAnchor = null;

    //移動速度の係数
    [SerializeField]
    private float _moveSpeed = 3;

    //現在の移動速度
    private Vector3 _currentVelocity = Vector3.zero;

    //=================================================================================
    //初期化
    //=================================================================================

    //コンポーネントがAddされた時に実行される
    private void Reset()
    {
        //中心のアンカー取得
        _centerEyeAnchor = transform.Find("TrackingSpace/CenterEyeAnchor");

        //Rigidbody取得、初期設定
        _rigidbody = gameObject.GetComponent<Rigidbody>();
        _rigidbody.useGravity = false;
        _rigidbody.freezeRotation = true;
    }

    //=================================================================================
    //更新
    //=================================================================================

    //入力はUpdateで確認
    private void Update()
    {
        //タッチパッドを触っている所の座標(-1 ~ 1)取得
        Vector2 primaryTouchpad = OVRInput.Get(OVRInput.Axis2D.PrimaryTouchpad);

        //Yの+の方向のMaxが0.5ぐらい(デバイスの不具合かも？)なので増やす
  

        if (primaryTouchpad.y > 0.5 && -0.5 < primaryTouchpad.x && primaryTouchpad.x < 0.5)
        {
            //向いてる方向、タッチパッドを触ってる場所から速度計算
            _currentVelocity = _centerEyeAnchor.rotation * new Vector3(primaryTouchpad.x, 0, primaryTouchpad.y);

            //上向いてる時に上にいっちゃうので上下方向の速度0に
            _currentVelocity.y = 0;

            //上下方向の速度を減らした分を左右に振るために正規化
            float speedMagnitude = _moveSpeed * primaryTouchpad.magnitude;//速度の大きさ
            _currentVelocity = _currentVelocity.normalized * speedMagnitude;
        }

        //物理演算関係(今回は速度)はFixedUpdateで設定

        }
    private void FixedUpdate()
    {
        _rigidbody.velocity = _currentVelocity;

    }
       
}