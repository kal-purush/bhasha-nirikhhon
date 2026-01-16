using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class GirlLeavebed : MonoBehaviour
{
    private Animator Anim;
    private GameObject Girl;
    private GameObject Hint;
    private GameObject SpaceHint;

    void Awake() {
        Anim = GetComponent<Animator>();
        Girl = GameObject.Find("Girl");
        SpaceHint = GameObject.Find("SpaceHint");
    }

    void Start()
    {
        Girl.SetActive(false);
        SpaceHint.SetActive(true);
        Anim.enabled = false;
    }

    void Update()
    {
        if (Input.GetKeyDown("space"))
        {
            SpaceHint.SetActive(false);
            Anim.enabled = true;
            StartCoroutine(WaitAnimDone());
        }
    }

    IEnumerator WaitAnimDone()
    {
        yield return new WaitWhile(() => Anim.GetCurrentAnimatorStateInfo(0).normalizedTime < 1);
        Girl.SetActive(true);
        gameObject.GetComponent<GirlLeavebed>().enabled = false;
    }

}