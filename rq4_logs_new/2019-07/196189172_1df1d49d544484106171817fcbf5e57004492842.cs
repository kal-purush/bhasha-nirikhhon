using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class CardBaseAgent : MonoBehaviour
{
    

    /// <summary>
    /// 逻辑层面成功后的下个卡片
    /// </summary>
    [SerializeField] protected CardBaseAgent _NextCard;
    public CardBaseAgent nextCard { set { _NextCard = value; } }

    CardStatus _cardStatus = CardStatus.InActive;

    public CardStatus cardStatus { set { _cardStatus = value; } get { return _cardStatus; } }


    /// <summary>
    /// 激活卡片
    /// </summary>
    public void DoActive() {
        gameObject.SetActive(true);
        _cardStatus = CardStatus.Active;
    }

    /// <summary>
    /// 准备阶段
    /// </summary>
    public virtual void DoPrepare() {
        CompletePrepare();
    }

    /// <summary>
    /// 入场
    /// </summary>
    public virtual void DoRunIn()
    {
        CompleteRunIn();
    }


    /// <summary>
    /// 运行阶段
    /// </summary>
    public virtual void DoRun() {
        Debug.Log("CardBaseAgent is runing!");
    }

    /// <summary>
    /// 出场
    /// </summary>
    public virtual void DoRunOut()
    {
        CompleteRunOut();
    }

    /// <summary>
    /// 结束阶段
    /// </summary>
    public virtual void DoEnd() {

    }

    protected void CompletePrepare() {
        _cardStatus = CardStatus.PrepareCompleted;
    }

    protected void CompleteRunIn()
    {
        _cardStatus = CardStatus.RunInCompleted;
    }

    protected void CompleteRunOut()
    {
        _cardStatus = CardStatus.RunOutCompleted;
    }

    protected void CompleteDoEnd()
    {
        _cardStatus = CardStatus.InActive;
    }



    void Start()
    {
        
    }

    // Update is called once per frame
    void Update()
    {
        if (_cardStatus == CardStatus.Active) {
            _cardStatus = CardStatus.Prepare;
        }

        if (_cardStatus == CardStatus.Prepare) {
            DoPrepare();
        }

        if (_cardStatus == CardStatus.PrepareCompleted) {
            DoRunIn();
        }

        if (_cardStatus == CardStatus.RunInCompleted) {
            DoRun();
        }

        if (_cardStatus == CardStatus.RunCompleted) {
            DoRunOut();
        }

        if (_cardStatus == CardStatus.RunOutCompleted)
        {
            DoEnd();
        }

    }
}