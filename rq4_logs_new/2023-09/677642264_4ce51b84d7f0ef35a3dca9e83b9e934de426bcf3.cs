using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class HeightLimit : MonoBehaviour
{
    [SerializeField]
    GameObject Player;
    [SerializeField]
    BoxCollider2D limiterCollider;
    // コライダーにコライダーがぶつかった時に始める処理
    void OnTriggerEnter2D(Collider2D collider)
    {
        // コライダーからゲームオブジェクトを経由してレイヤーを見に行く
        var otherLayer = collider.gameObject.layer; 
        // ぶつかった相手のレイヤーの名前を取得
        var otherLayerName = LayerMask.LayerToName(otherLayer); 

        switch (otherLayerName)
        {
            case "Player":
                {
                    // transform.positionにはVector2か3のインスタンスしか挿入できないので、positionの新たな値を設定するためのVector2インスタンスをまず作成する
                    Vector2 newPosition = new Vector2(Player.transform.position.x, this.transform.position.y - limiterCollider.size.y);
                    Player.gameObject.transform.position = newPosition;
                    break;
                }
            default:
                {
                    break;
                }
        }
    }
}