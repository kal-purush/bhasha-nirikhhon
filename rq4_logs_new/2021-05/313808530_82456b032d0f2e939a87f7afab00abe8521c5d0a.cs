using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.Playables;
using UnityEngine.SceneManagement;

public class StartTL : MonoBehaviour
{
    private GameObject TimeLine;
    private GameObject Player;
    private GameObject PlayerOnBed;
    private GameObject pen_paper;
    private GameObject SadFace;

    private GameObject PickUpHint;
    private GameObject paper;
    private GameObject pen;
    private GameObject board1;
    private GameObject board2;
    private GameObject board3;
    private GameObject board4;
    private GameObject board5;
    private GameObject LeaveTip;

    void Awake()
    {
        TimeLine = GameObject.Find("CometTimeline");
        Player = GameObject.Find("Player");
        PlayerOnBed = GameObject.Find("PlayerOnBed");
        pen_paper = GameObject.Find("BubblePenPaper");
        SadFace = GameObject.Find("SadFace");

        PickUpHint = GameObject.Find("PickUpHint");
        pen = GameObject.Find("BubblePen");
        paper = GameObject.Find("BubblePaper");
        board1 = GameObject.Find("board1");
        board2 = GameObject.Find("board2");
        board3 = GameObject.Find("board3");
        board4 = GameObject.Find("board4");
        board5 = GameObject.Find("board5");
        LeaveTip = GameObject.Find("LeaveTip");
    }

    void Start()
    {
        TimelineGameManager.GetDirector(TimeLine.GetComponent<PlayableDirector>());
        TimelineGameManager.isTimeline = true;
        Player.SetActive(false);
        pen_paper.SetActive(false);
        SadFace.SetActive(false);

        LeaveTip.SetActive(false);
        paper.SetActive(false);
        pen.SetActive(false);
        board1.SetActive(false);
        board2.SetActive(false);
        board3.SetActive(false);
        board4.SetActive(false);
        board5.SetActive(false);
        PickUpHint.SetActive(false);
    }

    // Update is called once per frame
    void Update()
    {
        
    }

    public void EndTimeline()
    {
        TimelineGameManager.isTimeline = false;
        TimeLine.GetComponent<PlayableDirector>().enabled = false;
        Dialog.PrintDialog("Level1 Start1");
        StartCoroutine(ShowBubble());
    }

    IEnumerator ShowBubble()
    {
        yield return new WaitWhile(GameManager.instance.IsDialogShow);
        pen_paper.SetActive(true);
        Dialog.PrintDialog("Level1 Start2");

        yield return new WaitWhile(GameManager.instance.IsDialogShow);
        PlayerOnBed.GetComponent<Animator>().SetTrigger("Down");

        yield return new WaitForSeconds(1.2f);
        PlayerOnBed.SetActive(false);
        Player.SetActive(true);
    }

}