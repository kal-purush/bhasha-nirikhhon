using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;
using UnityEngine.Playables;

public class GirlTL3Movement : MonoBehaviour
{
	private Rigidbody2D rb;
    private float moveH, moveV;
    [SerializeField] private float moveSpeed = 2f;
    private Animator GirlAnimator;
    public GameObject qMark;
    public GameObject Hint;
    private bool isCut;
    public static GameObject girlTimeline3;
    public GameObject fading;
    public Animator kingAnim;

    private void Awake()
    {
        rb = GetComponent<Rigidbody2D>();
        GirlAnimator = GetComponent<Animator>();
        girlTimeline3 = GameObject.Find("GirlTimeline");
        this.transform.position = GameManager.instance.PlayerPos;
    }
    // Start is called before the first frame update
    void Start()
    {
		TimelineGameManager.GetDirector(girlTimeline3.GetComponent<PlayableDirector>());
        girlTimeline3.SetActive(false);
        Hint.SetActive(false);
        fading.SetActive(false);
        isCut = false;
    }

    // Update is called once per frame
    void Update()
    {
        if (GameManager.instance.stopMoving) {
            rb.velocity = Vector2.zero;
        }
        else
        {
        	moveH = Input.GetAxisRaw("Horizontal");
	       // GirlAnimator.SetFloat("Direaction", moveH);
	        GirlAnimator.SetFloat("Speed", Mathf.Abs(moveH));
	        if(moveH < 0){
	            GirlAnimator.SetBool("FaceR", false);
	            //Debug.Log("FaceR is " + false);
	        }else if(moveH > 0){
	            GirlAnimator.SetBool("FaceR", true);
	            //Debug.Log("FaceR is " + true);
	        }
	        rb.velocity = new Vector2(moveH, 0) * moveSpeed; 
	        //割绳子
	        if (!isCut) {
	        	if (this.transform.position.x >= 42.65f && this.transform.position.x <= 43.1f) {
	        		Hint.SetActive(true);
			        if (Hint.activeSelf && Input.GetKeyDown("space")) {
			        	Debug.Log("可割绳子");
			        	Hint.SetActive(false);
			        	GirlAnimator.SetBool("isCut", true);
			        	GameManager.instance.stopMoving = true;
			        }
			    }
			    else{
			    	Hint.SetActive(false);
			    }
	        }
	    }

	    TimelineActive();
    }

    private void TimelineActive() {
    	if (isCut) {
			fading.SetActive(true);
			if (fading.GetComponent<Image>().color.a == 1 && !girlTimeline3.activeSelf) {
				girlTimeline3.SetActive(true);
				kingAnim.SetTrigger("isTimeline");
				GirlAnimator.SetBool("isCut", false);
				GirlAnimator.SetBool("FaceR", false);
    			this.transform.position = new Vector2(37.34f, -1.78f);
				this.GetComponent<SpriteRenderer>().flipX = true;
			}
			else if (girlTimeline3.GetComponent<PlayableDirector>().enabled == false) {
				Debug.Log("jieshu");
				LevelLoader.instance.LoadLevel("Level4Trace");
			}
    	}
    }
    //siginal
    public void falldown() {
    	GirlAnimator.SetTrigger("fall");
    }
    //玩家动画中
    void cutRope() {
    	isCut = true;
    }
}