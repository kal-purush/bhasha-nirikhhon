using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using Photon.Pun;

public class StoneThrowing : MonoBehaviourPunCallbacks
{

    /// <summary>
    /// 投げる石のオブジェクト
    /// </summary>
    [SerializeField] GameObject stone;

    /// <summary>
    /// 石のリジッドボディ
    /// </summary>
    Rigidbody2D stoneRb;

    /// <summary>
    /// 投げる際にImpulseで与える力
    /// </summary>
    [SerializeField] float throwPower = 1.0f;

    [SerializeField] Vector2 throwDirection = new Vector2(1, 1);

    [SerializeField] Transform CreatePos;

    /// <summary>
    /// 投げる向き　true: 右　false:左
    /// </summary>
    public bool sideways = true;

    /// <summary>
    /// 石を投げる
    /// </summary>
    public void ThrowStone()
    {

        if ((sideways && throwDirection.x < 0) || (!sideways && throwDirection.x > 0))
        {
            throwDirection.x *= -1;
        }

        GameObject stoneInstance = Instantiate(stone);
        //stoneInstance.transform.position = CreatePos.position;
        stoneRb = stoneInstance.GetComponent<Rigidbody2D>();
        stoneRb.AddForce(throwDirection * throwPower, ForceMode2D.Impulse);

    }
}