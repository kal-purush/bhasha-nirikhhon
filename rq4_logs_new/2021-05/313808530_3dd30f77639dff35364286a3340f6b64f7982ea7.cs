using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class StartDialog : MonoBehaviour
{
    private GameObject Soldier1;
    private GameObject Soldier2;
    private GameObject Soldier3;
    private GameObject Painting1;
    private GameObject Painting2;
    private GameObject Painting3;
    [SerializeField] private Transform StartPoint;
    private bool IsStarted;
    public static bool IsStand;
    private GameObject KeyHint;

    void Awake()
    {
        Soldier1 = GameObject.Find("Soldier");
        Soldier2 = GameObject.Find("Soldier2");
        Soldier3 = GameObject.Find("Soldier3");
        Painting1 = GameObject.Find("Drawing");
        Painting2 = GameObject.Find("Drawing2");
        Painting3 = GameObject.Find("Drawing3");
        KeyHint = GameObject.Find("KeyHint");
    }

    void Start()
    {
        Painting1.GetComponent<DrawingTrigger>().enabled = false;
        Painting2.GetComponent<DrawingTriggerRe>().enabled = false;
        Painting3.GetComponent<DrawingTriggerRe>().enabled = false;
        IsStarted = false;
        IsStand = false;
        KeyHint.SetActive(false);
    }

    void Update()
    {
        if (this.gameObject.transform.position.x > StartPoint.position.x && !IsStarted)
        {
            IsStand = true;
            this.gameObject.GetComponent<GirlAction>().enabled = false;
            // Soldier1.GetComponent<ThreeSoldierAction>().enabled = false;
            // Soldier2.GetComponent<ThreeSoldierAction>().enabled = false;
            // Soldier3.GetComponent<ThreeSoldierAction>().enabled = false;
            this.gameObject.GetComponent<Rigidbody2D>().velocity = Vector2.zero;
            this.gameObject.GetComponent<Animator>().enabled = false;
            this.gameObject.GetComponent<SpriteRenderer>().sprite = Resources.Load<Sprite>("Level4/GirlHat/A_GirlHatMove00");
            // Soldier1.GetComponent<Rigidbody2D>().velocity = Vector2.zero;
            // Soldier2.GetComponent<Rigidbody2D>().velocity = Vector2.zero;
            // Soldier3.GetComponent<Rigidbody2D>().velocity = Vector2.zero;
            Dialog.PrintDialog("Lv4Part2Start");
            StartCoroutine(CheckDialogDone());
            IsStarted = true;
        }
    }

    IEnumerator CheckDialogDone()
    {
        yield return new WaitWhile(GameManager.instance.IsDialogShow);
        Painting1.GetComponent<DrawingTrigger>().enabled = true;
        Painting2.GetComponent<DrawingTriggerRe>().enabled = true;
        Painting3.GetComponent<DrawingTriggerRe>().enabled = true;
        this.gameObject.GetComponent<GirlAction>().enabled = true;
        // Soldier1.GetComponent<ThreeSoldierAction>().enabled = true;
        // Soldier2.GetComponent<ThreeSoldierAction>().enabled = true;
        // Soldier3.GetComponent<ThreeSoldierAction>().enabled = true;
        IsStand = false;
        KeyHint.SetActive(true);
        this.gameObject.GetComponent<StartDialog>().enabled = false;
    }
}