using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.SceneManagement;

public class GirlTimeLineMovement : MonoBehaviour
{
	private Rigidbody2D rb;
    private float moveH, moveV;
    private Vector2 direction;
    [SerializeField] private float moveSpeed = 2f;
    private Animator GirlAnimator;
    public static bool isPlayFailUI;

    private void Awake()
    {
        rb = GetComponent<Rigidbody2D>();
        GirlAnimator = GetComponent<Animator>();
    }

    // Start is called before the first frame update
    void Start()
    {
    	isPlayFailUI = false;
    }

    // Update is called once per frame
    void FixedUpdate()
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

	        direction = new Vector2(moveH, 0);
	        rb.velocity = direction * moveSpeed; 
	    }
    }

    void OnTriggerEnter2D(Collider2D collision) {
    	if (collision.gameObject.name == "InvisibleWall02") {
	        GirlAnimator.SetFloat("Speed", 0.0f);
        	TimelineGameManager.isTimeline = true;
        	if (SceneManager.GetActiveScene().name == "Level4P2TL1") {
            	SoliderTimeline.girlTimeLine.SetActive(true);
        	} else if (SceneManager.GetActiveScene().name == "Level4P2TL2") {
            	SoliderTimeline2.girlTimeLine.SetActive(true);
            }
            Destroy(collision);
    	}
    }

    public void AnimTrig() {
    	GirlAnimator.SetTrigger("Catched");
    }

    public void Failed() {
        isPlayFailUI = true;
    }
}