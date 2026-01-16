using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class PlayerController : MonoBehaviour
{

    #region Variables
    public float Speed;
    public float JumpForce;
    private float moveinput;
    public GameObject player;
    public GameObject Player;

    public float jumpMultiplierWater = 1f;
    public float speedMultiplierWater = 1f;
    public float jumpMultiplierStone = 1f;
    public float speedMultiplierStone = 1f;
    public float jumpMultiplierFire = 1f;
    public float speedMultiplierFire = 1f;
    private float canJump = 0f;

    public Animator animator;
    float InputX;
    float InputY;


    private Rigidbody2D rb;
    private SpriteRenderer mySpriteRenderer;
    



    public Sprite waterSprite;
    public Sprite fireSprite;
    public Sprite stoneSprite;

    private Rigidbody2D rigidbody2d;


    public bool isGrounded = true;
    public Transform groundCheck;
    public float checkRadius;
    public LayerMask whatIsGround;

    public int extraJumps;

    public AudioSource stoneMove;
    public AudioSource waterMove;
    public AudioSource fireMove;
    public AudioSource fireJump;
    public AudioSource waterJump;
    public AudioSource stoneJump;

    #endregion

    private void Awake()
    {
        mySpriteRenderer = GetComponent<SpriteRenderer>();
        rigidbody2d = GetComponent<Rigidbody2D>();
    }

    void Start()
    {
        rb = GetComponent<Rigidbody2D>();

        animator = this.gameObject.GetComponent<Animator>();


    }


    void FixedUpdate()
    {
        isGrounded = Physics2D.OverlapCircle(groundCheck.position, checkRadius, whatIsGround);

        moveinput = Input.GetAxisRaw("Horizontal");
        //rb.velocity = new Vector2(moveinput * Speed, rb.velocity.y);
        rb.AddForce(new Vector2(moveinput, 0) * Speed, ForceMode2D.Force);

        float horizontal = Input.GetAxis("Horizontal");


       
       
    }



   

    void OnTriggerEnter2D(Collider2D other)
    {

        if (other.gameObject.tag == "Water")
        {

            changeToWater();
            GetComponent<SpriteRenderer>().sprite = waterSprite;
            StartCoroutine(ResetPlayer(1));
            animator.SetTrigger("waterOrb");



        }
        else if (other.gameObject.tag == "Fire")
        {

            changeToFire();
            GetComponent<SpriteRenderer>().sprite = fireSprite;
            StartCoroutine(ResetPlayer(1));
            animator.SetTrigger("fireOrb");





        }
        else if (other.gameObject.tag == "Stone")
        {

            changeToStone();
            GetComponent<SpriteRenderer>().sprite = stoneSprite;
            StartCoroutine(ResetPlayer(1));
            animator.SetTrigger("stoneOrb");


        }
        else if (other.gameObject.tag == "WaterAbsorb")
        {

            animator.SetTrigger("waterEnter");


        }
        else if (other.gameObject.tag == "WaterDumb")
        {

            animator.SetTrigger("waterDump");

        }








    }

    public void changeToWater()
    {
        gameObject.tag = "Water";
        //JumpForce = JumpForce * jumpMultiplierWater;
        //Speed = Speed * speedMultiplierWater;
        JumpForce = 20f;
        Speed = 28f;



    }

    public void changeToFire()
    {
        gameObject.tag = "Fire";
        //JumpForce = JumpForce * jumpMultiplierFire;
        //Speed = JumpForce * speedMultiplierFire;
        JumpForce = 30f;
        Speed = 35f;

    }

    public void changeToStone()
    {
        gameObject.tag = "Stone";
        //JumpForce = JumpForce * jumpMultiplierStone;
        //Speed = JumpForce * speedMultiplierStone;
        JumpForce = 10f;
        Speed = 22f;


    }



    void Update()
    {

        if (isGrounded == true)
        {
            extraJumps = 1;
            
        }








        if ((Input.GetKeyDown(KeyCode.UpArrow) || Input.GetKeyDown(KeyCode.W) && extraJumps > 0 && Time.time > canJump))
        {
           //Change timer below for shorter or longer jumpCooldown
            canJump = Time.time + 0.5f;

            rb.velocity = Vector2.up * JumpForce;
            extraJumps--;

            print("jump");

            if (Player.gameObject.tag == "Water")
            {
                waterJump.Play();
            }

            if (Player.gameObject.tag == "Stone")
            {
                stoneJump.Play();
            }

            if (Player.gameObject.tag == "Fire")
            {
                fireJump.Play();
            }
        }
            else if ((Input.GetKeyDown(KeyCode.UpArrow) || Input.GetKeyDown(KeyCode.W)  && extraJumps == 0 && isGrounded == true))
        {
            rb.velocity = Vector2.up * JumpForce;

            
            
            if (Player.gameObject.tag == "Water")
            {
                waterJump.Play();
            }

            if (Player.gameObject.tag == "Stone")
            {
                stoneJump.Play();
            }

            if (Player.gameObject.tag == "Fire")
            {
                fireJump.Play();
            }
        }

        //if (rb.velocity.y < 0){
        //    rb.velocity += Vector2.up * Physics2D.gravity.y * (fallMultiplier - 1) * Time.deltaTime;

        //}
        //else if (rb.velocity.y > 0 && !Input.GetButton ("Jump"))
        //{

        //    rb.velocity += Vector2.up * Physics2D.gravity.y * (fallMultiplier - 1) * Time.deltaTime;
        //}




        InputX = Input.GetAxis("Horizontal");
        animator.SetFloat("InputX", InputX);


        InputY = Input.GetAxis("Vertical");
        animator.SetFloat("InputY", InputY);





        if (rigidbody2d.velocity.x >= 0)
        {
            mySpriteRenderer.flipX = false;
        }
        else
        {
            mySpriteRenderer.flipX = true;
        }


        if (rb.velocity.magnitude >= 0.1)
        {

            if (Player.gameObject.tag == "Water")
            {
                waterMove.Play();
            }

            if (Player.gameObject.tag == "Stone")
            {
                stoneMove.Play();
            }

            if (Player.gameObject.tag == "Fire")
            {
                fireMove.Play();
            }

        }



    }




   
    private IEnumerator ResetPlayer(float time)
    {




        Vector3 originalScale = Player.transform.localScale;
        Vector3 destinationScale = new Vector3(1f, 1f, 1f);


        float currentTime = 0.0f;

        do
        {
            Player.transform.localScale = Vector3.Lerp(originalScale, destinationScale, currentTime / time);
            currentTime += Time.deltaTime;
            yield return null;
        } while (currentTime <= time);




    }








}




