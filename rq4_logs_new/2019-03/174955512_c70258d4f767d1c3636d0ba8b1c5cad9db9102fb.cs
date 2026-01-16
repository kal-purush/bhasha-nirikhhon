using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.EventSystems;
using UnityEngine.SceneManagement;

public class PlayStartButton : ButtonPointerBase
{
    [SerializeField]
    GameMaster master;
    Animator anim;

    private void Start()
    {
        anim = GetComponent<Animator>();
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
        master.GameStart();
        transform.parent.gameObject.SetActive(false);
    }
}