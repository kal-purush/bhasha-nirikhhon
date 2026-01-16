using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.EventSystems;
using UnityEngine.SceneManagement;

public class TweetButton : ButtonPointerBase
{
    Animator anim;
    RTweet tw;
    TwiMessageGen gen;
    string message = "";
    private void Start()
    {
        anim = GetComponent<Animator>();
        tw = new RTweet();
        gen = new TwiMessageGen();
    }

    public override void OnPointerDown(PointerEventData eventData)
    {
        anim.SetTrigger("Pushed");
    }

    public override void OnPointerEnter(PointerEventData eventData)
    {
        anim.SetBool("OnMouse", true);
    }

    public override void OnPointerExit(PointerEventData eventData)
    {
        anim.SetBool("OnMouse", false);
    }

    public override void OnPointerClick(PointerEventData eventData)
    {
        tw.Tweet(gen.GetMessage(10000));
    }
}