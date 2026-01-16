using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class MergeState : IState
{
    float timer = 0;
    public void OnEnter(GameManager gM)
    {
        timer = 0;
    }

    public void OnExecute(GameManager gM)
    {
        timer += Time.deltaTime;
        if (timer >= 1.5) gM.ChangeState(new DropState());
    }

    public void OnExit(GameManager gM)
    {
        gM.endBlock.Anim.Play("MergeBlock");
        gM.endBlock.gameObject.GetComponent<SpriteRenderer>().sortingOrder = 2;
        gM.endBlock.gameObject.GetComponentInChildren<Canvas>().sortingOrder = 3;
        int number = gM.endBlock.Number;
        gM.endBlock.NumberText.text = gM.numberSO.listNumber[number].number.ToString();
        gM.endBlock.GetComponent<SpriteRenderer>().material.color = gM.numberSO.listNumber[number].color;
    }
}