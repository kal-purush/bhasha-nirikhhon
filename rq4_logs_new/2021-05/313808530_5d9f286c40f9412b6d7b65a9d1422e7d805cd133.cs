using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class CallRhythm : MonoBehaviour
{
    [SerializeField] GameObject Rhythm;
    [SerializeField] BeatScrollerRe beatScrollerRe;
    [SerializeField] BirdOutDoorMovement birdOutDoorMovement;
    [SerializeField] bool IsInFace = false;
    public bool IsGameEnded;
    private GameObject SpaceHint;
    private GameObject RhythmHint;

    void Awake() 
    {
        Rhythm = GameObject.Find("RhythmGameUI");
        SpaceHint = GameObject.Find("SpaceHint");
        RhythmHint = GameObject.Find("RhythmHint");
    }

    void Start()
    {
        Rhythm.SetActive(false);
        IsGameEnded = false;
        RhythmHint.SetActive(false);
        SpaceHint.SetActive(false);
    }

    void Update()
    {
        if (IsInFace)
        {
            if (Input.GetKeyDown("space"))
            {
                GameManager.instance.stopMoving = true;
                SpaceHint.SetActive(false);
                
                Rhythm.SetActive(true);
                RhythmHint.SetActive(true);
                beatScrollerRe.score = 0;
                beatScrollerRe.total = 0;
                IsInFace = false;
            }
        }

        if (beatScrollerRe.Reset)
        {
            Rhythm.SetActive(false);
            RhythmHint.SetActive(false);
            if (beatScrollerRe.score >= 4)
            {
                IsGameEnded = true;
            }
            GameManager.instance.stopMoving = false;
            beatScrollerRe.Reset = false;
        }
    }

    //当bird撞上sadFace时触发五线谱
    void OnTriggerEnter2D(Collider2D other) {
    	if (other.tag.CompareTo("Player") == 0 && !Rhythm.activeSelf) {
            IsInFace = true;
            SpaceHint.SetActive(true);
    	}
    }
    void OnTriggerExit2D(Collider2D other) {
        if (other.tag.CompareTo("Player") == 0){
            IsInFace = false;
            SpaceHint.SetActive(false);
        }
    }
}