using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class BombGenerator : MonoBehaviour
{
    //爆弾のプレハブ化
    [SerializeField] GameObject BombPrefab;

    //時間関連----------------------------------------------------------------------------------------------------------------------------------------------------------------------

    //最初の爆弾生成時間
    [SerializeField] float FirstGenerationTime = 5f;
    //最初の生成をするための経過時間
    [SerializeField] float First_time = 0f;
    //経過時間
    [SerializeField] private float time = 0f;
    //爆弾生成時間間隔
    [SerializeField] float time_interval = 10f;
    //生成時間の間隔を狭める
    [SerializeField] float GetFasterTime = 0.5f;
    //最低限の生成時間
    [SerializeField] float FinishGenerationTime = 2f;

    //爆弾の座標関連------------------------------------------------------------------------------------------------------------------------------------------------------------------
    
    //X座標の最小値
    [SerializeField] float xMinPosition = -10f;
    //X座標の最大値
    [SerializeField] float xMaxPosition = 10f;
    //Z座標の最大値
    [SerializeField] float zMinPosition = -10f;
    //Z座標の最小値
    [SerializeField] float zMaxPosition = 10f;

    //爆弾生成フラグ
    [SerializeField] bool BombGenerationOK = false;



    void Update()
    {
        if (!BombGenerationOK)
        {
            First_time += Time.deltaTime;

            if(First_time > FirstGenerationTime)
            {
                GameObject FirstBomb = Instantiate(BombPrefab);
                FirstBomb.transform.position = GetRandomPosition();
                First_time = 0f;
                BombGenerationOK = true;
            }
        }

        if (BombGenerationOK)
        {
            time += Time.deltaTime;

            if (time > time_interval)
            {
                GameObject Bomb = Instantiate(BombPrefab);

                Bomb.transform.position = GetRandomPosition();

                time = 0f;

                if (time_interval != FinishGenerationTime)
                {
                    time_interval = GenerationTime();
                }
            }
        }
    }

    private float GenerationTime()
    {
        return time_interval -= GetFasterTime;
    }

    private Vector3 GetRandomPosition()
    {
        float x = Random.Range(xMinPosition, xMaxPosition);
        float z = Random.Range(zMinPosition, zMaxPosition);

        return new Vector3(x, 0, z);
    }
}