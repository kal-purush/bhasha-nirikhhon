using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class RhythmController : MonoBehaviour
{
    private GameObject Rhythm1, Rhythm2, Rhythm3, Rhythm4;
    [SerializeField] BeatScrollerRe beatScrollerRe1, beatScrollerRe2, beatScrollerRe3, beatScrollerRe4;
    private GameObject Fail;
    private GameObject Hint;
    public bool IsFailed;

    void Awake()
    {
        Rhythm1 = GameObject.Find("Rhythm1");
        Rhythm2 = GameObject.Find("Rhythm2");
        Rhythm3 = GameObject.Find("Rhythm3");
        Rhythm4 = GameObject.Find("Rhythm4");
        Fail = GameObject.Find("Fail");
        Hint = GameObject.Find("Hint");
    }

    void Start()
    {
        Rhythm1.SetActive(true);
        Fail.SetActive(false);
        Hint.SetActive(false);
        IsFailed = false;
    }

    void Update()
    {
        if (beatScrollerRe1.total == 5)
        {
            Rhythm1.SetActive(false);
            if (beatScrollerRe1.score >= 4)
            {
                // IsGameEnded = true;
            }
            else
            {
                IsFailed = true;
                Fail.SetActive(true);
                StartCoroutine(WaitanimDone());
            }
        }

        if (beatScrollerRe2.total == 5)
        {
            Rhythm2.SetActive(false);
            if (beatScrollerRe2.score >= 4)
            {
                // IsGameEnded = true;
            }
            else
            {
                IsFailed = true;
                Fail.SetActive(true);
                StartCoroutine(WaitanimDone());
            }
        }

        if (beatScrollerRe3.total == 5)
        {
            Rhythm3.SetActive(false);
            if (beatScrollerRe3.score >= 4)
            {
                // IsGameEnded = true;
            }
            else
            {
                IsFailed = true;
                Fail.SetActive(true);
                StartCoroutine(WaitanimDone());
            }
        }

        if (beatScrollerRe4.total == 5)
        {
            Rhythm4.SetActive(false);
            if (beatScrollerRe4.score >= 4)
            {
                // IsGameEnded = true;
            }
            else
            {
                IsFailed = true;
                Fail.SetActive(true);
                StartCoroutine(WaitanimDone());
            }
        }
    }

    IEnumerator WaitanimDone()
    {
        yield return new WaitWhile(() => Fail.GetComponent<Animator>().GetCurrentAnimatorStateInfo(0).normalizedTime < 1);
        
        if (Input.GetKeyDown("space"))
        {
            // Fail.SetActive(false);
            // Hint.SetActive(false);
            LevelLoader.instance.LoadLevel("Level5");
        }else{
            Hint.SetActive(true);
        }
    }
}