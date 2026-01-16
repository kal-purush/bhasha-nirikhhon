using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.EventSystems;
using UnityEngine.SceneManagement;
using UnityEngine.UI;


public class PlayerNew : MonoBehaviour
{

    //Player Lifepoint
    private int lifePoint = 2;
    private int coin = 0;

    //Movement
    public float MoveSpeed = 10;
    float CurrentMoveSpeed = 0;
    public float SlowdownMoveSpeed = 5;

    //public float TurnRate = 2f;
    public Vector3 moveVector;
    public double SpeedIncreaseRate = 0.05;
    public float MaxSpeed = 25;
    public float SwipeSpeedLimit = 0.75f;
    float dirX;

    //Jumping
    private float fall = 2.5f;
    private float lowJump = 2f;
    private bool isGrounded;
    private float jumpSpeed = 7f;
    private float touchJumpSpeed = 10f;

    //Bullet asset
    public GameObject Bullet;
    public float BulletForce = 100000;
    public Camera playerCam;

    private float animationDuration = 1.0f;

    //Android jumping control
    private Vector3 fp;   //First touch position
    private Vector3 lp;   //Last touch position
    private float dragDistance = Screen.height * 5 / 100;

    //Buffs
    public static bool isSlowedDown = false;
    public static bool isDashing = false;
    public static bool isTeleporting = false;
    public static float speedUp = 20;

    Rigidbody rb;

    void Awake()
    {
        // Call the LevelManager and set the last level.
        LoadPrevScene.setLastLevel(SceneManager.GetActiveScene().name);
        Debug.Log(SceneManager.GetActiveScene().name.ToString());
    }

    // Use this for initialization
    void Start()
    {
        if (SceneManager.GetActiveScene().name.Equals("EndlessMode"))
        {
            GameMaster.SetLife(0);
        }
        else if (SceneManager.GetActiveScene().name.Equals("TutorialLevel"))
        {
            GameMaster.SetLife(99);
        }
        else
        {
            GameMaster.SetLife(3);
        }
        ScoreManager.SetScore(0);
        ScoreManager.SetCoin(0);
        rb = GetComponent<Rigidbody>();
        //Cursor.lockState = CursorLockMode.Locked;
    }

    int countFrame = 0;

    private void FixedUpdate()
    {
        if (countFrame % 60 == 0 && MoveSpeed < MaxSpeed)
        {
            //Increase speed as the time goes by
            MoveSpeed += (float)SpeedIncreaseRate;
            //Debug.Log("Current movespeed:" + MoveSpeed);
        }
        countFrame++;
    }

    void Update()
    {

        // jump improve
        if (rb.velocity.y < 0)
        {
            rb.velocity += Vector3.up * Physics2D.gravity.y * (fall - 1) * Time.deltaTime;
        }
        else if (rb.velocity.y > 0 && !Input.GetButton("Jump"))
        {
            rb.velocity += Vector3.up * Physics2D.gravity.y * (lowJump - 1) * Time.deltaTime;
        }

        //Jumping
        if (Input.GetButton("Jump"))
        {
            Jump(jumpSpeed);
        }

        //For debugging
        if (Input.GetMouseButtonDown(0)) //button 0 is left click and 1 is right click
        {
            //Dash();
        }

        checkJump();

        MovePlayerFromInputs();

        addTimeScore();
    }


    void checkJump()
    {
        if (Input.touchCount == 1) // user is touching the screen with a single touch
        {
            Touch touch = Input.GetTouch(0); // get the touch
            if (touch.phase == TouchPhase.Began) //check for the first touch
            {
                fp = touch.position;
                lp = touch.position;
            }
            else if (touch.phase == TouchPhase.Moved) // update the last position based on where they moved
            {
                lp = touch.position;
            }
            else if (touch.phase == TouchPhase.Ended) //check if the finger is removed from the screen
            {
                lp = touch.position;  //last touch position. Ommitted if you use list

                //Check if drag distance is greater than dragDistance of the screen height
                if (Mathf.Abs(lp.x - fp.x) > dragDistance || Mathf.Abs(lp.y - fp.y) > dragDistance)
                {
                    //It's a drag
                    //the vertical movement is greater than the horizontal movement
                    if (Mathf.Abs(lp.x - fp.x) < Mathf.Abs(lp.y - fp.y))
                    {
                        if (lp.y > fp.y)
                        {
                            //Up swipe
                            Debug.Log("Up Swipe");
                            Jump(touchJumpSpeed);
                        }
                        else
                        {
                            //Down swipe
                            //Dashing is unused
                            //Dash();
                            Debug.Log("Down Swipe");
                        }
                    }
                }
                else
                {   //It's a tap as the drag distance is less than dragDistance of the screen height
                    //ShootBullet is unused.
                    //ShootBullet();
                }
            }
        }
    }

    void addTimeScore()
    {
        ScoreManager.AddScore(Time.deltaTime);
    }


    /*
     * 
     * Codes below this comment are
     * for input controlling functions
     * 
     */


    void ShootBullet()
    {
        GameObject temp = Instantiate(Bullet, new Vector3(transform.position.x - 3, transform.position.y, transform.position.z), playerCam.transform.rotation);
        temp.GetComponent<Rigidbody>().velocity = playerCam.transform.forward * BulletForce * 100;
    }

    void MovePlayerFromInputs()
    {
        //Touch screen
        if (Input.touchCount == 1) // user is touching the screen with a single touch
        {
            Touch touch = Input.GetTouch(0); // get the touch
            if (touch.phase == TouchPhase.Began) //check for the first touch
            {
                fp = touch.position;
                lp = touch.position;
            }
            else if (touch.phase == TouchPhase.Moved) // update the last position based on where they moved
            {
                lp = touch.position;
            }
            else if (touch.phase == TouchPhase.Ended) //check if the finger is removed from the screen
            {
                lp = touch.position;  //last touch position. Ommitted if you use list
                //Check if drag distance is greater than dragDistance of the screen height
                if (Mathf.Abs(lp.x - fp.x) > dragDistance || Mathf.Abs(lp.y - fp.y) > dragDistance)
                {
                    //It's a drag
                    //the vertical movement is greater than the horizontal movement
                    if (Mathf.Abs(lp.x - fp.x) < Mathf.Abs(lp.y - fp.y))
                    {
                        if (lp.y > fp.y)
                        {
                            //Up swipe
                            Debug.Log("Up Swipe");
                            Jump(touchJumpSpeed);
                        }
                        else
                        {
                            //Down swipe
                            //Dashing is unused
                            //Dash();
                            Debug.Log("Down Swipe");
                        }
                    }
                }

            }
        }
        //Keyboard move
        if (Input.GetAxis("Vertical") < 0)
        {
            //TODO: Redo this teleport condition
            //Teleport();
        }
        else
        {
            transform.Translate(new Vector3(-1, 0f, Input.GetAxis("Horizontal")) * MoveSpeed * Time.deltaTime, Space.Self);
        }
    }

    void Jump(float speed)
    {
        if (isGrounded)
        {
            rb.AddForce(new Vector3(0, 1, 0) * speed, ForceMode.Impulse);
            if (isDashing)
            {
                CancelDash();
            }
            isGrounded = false;
        }
    }

    /*
    void Dash()
    {
        Debug.Log("Dashing called");
        if (isGrounded && !isDashing)
        {
            Debug.Log("Dashing started");
            isDashing = true;
            //TODO: Animations??
            transform.localScale += new Vector3(0, -0.5f, 0);
            Invoke("CancelDash", 1);
        }
    }

    void CancelDash()
    {
        Debug.Log("Dashing ended");
        if (isDashing)
        {
            transform.localScale += new Vector3(0, +0.5f, 0);
        }
        isDashing = false;
    }
    */

    public void Slowdown()
    {

        Debug.Log("Slowdown called");
        if (!isSlowedDown)
        {
            CurrentMoveSpeed = MoveSpeed;
            MoveSpeed = SlowdownMoveSpeed;
            Debug.Log("Slowdown started");
            isSlowedDown = true;
            Invoke("CancelSlowdown", 1);
        }
    }

    void CancelSlowdown()
    {
        Debug.Log("Slowdown ended");
        if (isSlowedDown)
        {
            MoveSpeed = CurrentMoveSpeed;
        }
        isSlowedDown = false;
    }


    /*
     * 
     * These are for collision detection
     * 
     */

    private void OnCollisionEnter(Collision collision)
    {
        if (isTeleporting)
        {
            if (!collision.gameObject.tag.Equals("Ground") && !collision.gameObject.tag.Equals("LWall") && !collision.gameObject.tag.Equals("RWall"))
            {
                Physics.IgnoreCollision(GetComponent<Collider>(), collision.collider);
                Physics.IgnoreCollision(GetComponentInChildren<Collider>(), collision.collider);
            }
        }
        else
        {
            if (collision.gameObject.tag.Equals("Ground"))
            {
                if (!isGrounded)
                {
                    isGrounded = true;
                }
            }

            if (collision.gameObject.tag.Equals("Monster") && !isTeleporting)
            {
                Slowdown();
            }

            fallDamage.setSavePoint(transform.position.x, transform.position.y, transform.position.z);
        }

    }

}