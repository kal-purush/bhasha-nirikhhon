using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.Playables;
using UnityEngine.SceneManagement;

public class RunInPalace : MonoBehaviour
{
    private Animator GirlAnim;
    private Animator BrownAnim;
    private GameObject TimeLine2;
    private GameObject BrownMan;
    private GameObject NPC;

    void Awake()
    {
        GirlAnim = this.gameObject.GetComponent<Animator>();
        BrownAnim = GameObject.Find("BrownMan").GetComponent<Animator>();
        TimeLine2 = GameObject.Find("PalaceTimeline");
        BrownMan = GameObject.Find("BrownMan");
        NPC = GameObject.Find("NPC");
    }

    void Start()
    {
        BrownMan.GetComponent<SpriteRenderer>().flipX = true;
        GirlAnim.SetTrigger("Up");
        BrownAnim.SetTrigger("Help");
        StartCoroutine(WaitanimDone());
    }

    void Update()
    {

    }

    IEnumerator WaitanimDone()
    {
        yield return new WaitForSeconds(1.2f);
        Dialog.PrintDialog("Lv4Part3TL1");
        StartCoroutine(WaitDialogDone());
    }

    IEnumerator WaitDialogDone()
    {
        yield return new WaitWhile(GameManager.instance.IsDialogShow);
        
        TimeLine2.SetActive(true);
        TimelineGameManager.GetDirector(TimeLine2.GetComponent<PlayableDirector>());
        TimelineGameManager.isTimeline = true;
    }

    public void EndDialog()
    {
        TimelineGameManager.isTimeline = false;
        TimeLine2.GetComponent<PlayableDirector>().enabled = false;
        BrownMan.SetActive(false);
        NPC.SetActive(false);
        Dialog.PrintDialog("Lv4Part3TL2");
        StartCoroutine(WaitDialog2Done());
    }

    IEnumerator WaitDialog2Done()
    {
        yield return new WaitWhile(GameManager.instance.IsDialogShow);
        LevelLoader.instance.LoadLevel("Level5");
    }
}