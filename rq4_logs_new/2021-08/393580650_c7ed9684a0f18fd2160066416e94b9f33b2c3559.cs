using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UniRx;
using System;

public class Player1HP : MonoBehaviour
{
    /// <summary>
    /// playerのHP
    /// </summary>
    [SerializeField] int m_hp = 10;
    /// <summary>
    /// HPのリアクティブプロパティ
    /// </summary>
    ReactiveProperty<int> hp = new ReactiveProperty<int>();
    /// <summary>
    /// イベント発行のインスタンス
    /// </summary>
    private Subject<Unit> playerHpSubject = new Subject<Unit>();
    /// <summary>
    /// イベントの購読のみ公開
    /// </summary>
    public IObservable<Unit> PlayerDeth => playerHpSubject;
    /// <summary>
    /// 弾のタグ
    /// </summary>
    [SerializeField] string m_bulletTag;
    private void Start()
    {
        hp.Value = m_hp;
    }

    private void OnCollisionEnter(Collision collision)
    {
        if (collision.collider.CompareTag(m_bulletTag))
        {
            hp.Value -= 1;
        }
        if (hp.Value <= 0)
        {
            playerHpSubject.OnNext(Unit.Default);
        }
    }

    private void Update()
    {
        if (Input.GetKeyDown(KeyCode.Space))
        {
            hp.Value -= 5;
        }

        if (hp.Value <= 0)
        {
            playerHpSubject.OnNext(Unit.Default);
        }
    }
}