using UnityEngine;
using System.Collections;

public class BoardController : MonoBehaviour 
{
	public SpriteRenderer spriteRenderer = null;
	private Animator boardAnimator = null;
	private Sprite[] boardSprite = null;

    void Awake() {
        Initvariable();
    }

    void Start() {
        BindFuncToEvent();
    }

    void Initvariable() {
        boardAnimator = GetComponent<Animator>();
        boardSprite = Resources.LoadAll<Sprite>("Image/Board");
    }

    void BindFuncToEvent() {
        EventManager.instacne.AddFuncToEventForStart(Move1, EventManager.EventType.SPECIAL);
        EventManager.instacne.AddFuncToEventForStart(Move2, EventManager.EventType.BRANCH);
    }

	void Move1()
	{
		spriteRenderer.sprite = boardSprite [0];
		boardAnimator.SetTrigger ("Move");
	}

	void Move2()
	{
		spriteRenderer.sprite = boardSprite [1];
		boardAnimator.SetTrigger ("Move");
	}
}