using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using DG.Tweening;

public class MoveItem : MonoBehaviour
{
    // Start is called before the first frame update
    void Start()
    {
        
    }

    void OnEnable()
    {
        double Y = UnityEngine.Random.value;
        this.gameObject.transform.position = new Vector3(-4f, (float)Y*5f-2f, 0f); //-2~3までのY座標に出現 
        this.gameObject.transform.DOMove(new Vector3(4f, (float)Y*(-5f)+3f, 0f), 3f).SetEase(Ease.Linear); //反対側に移動
    }

    void OnDisable()
    {
        this.transform.DOKill(); // 動きを止める
        this.gameObject.transform.position = new Vector3(-4f, 0f, 0f); //位置リセット 
    }

    // Update is called once per frame
    void Update()
    {
        
    }
}