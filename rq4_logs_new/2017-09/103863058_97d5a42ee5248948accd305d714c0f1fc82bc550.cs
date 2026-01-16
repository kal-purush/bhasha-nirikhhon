using System.Collections;
using System.Collections.Generic;
using UnityEngine;
public class AIBehaviour : MonoBehaviour {

    #region Variables

    //How much force should be applied while I jump
    public float jumpForce;
    //My movement speed
    public float moveSpeed;
    //My initial facing position is towards the left
    bool facingLeft = true;
    Rigidbody2D rigidBody;
    GameObject ball;
    Rigidbody2D ballRigidbody;
    //Center of my side of the court
    public Transform center;
    //How much force should be applied to the ball
    public float ballForce;
    //Ground Layermask
    public LayerMask whatIsGround;
    float groundCheckRadius = 0.2f;
    public Transform groundChecker;
    Animator anim;
    //How much times am I allowed to jump
    int jumpCount = 0;
    //Max times I can jump for called "Foul"
    const int maxJumpCount = 3;
    //Am I touching the ground
    bool grounded;
    //Time before I'm allowed to next jump...Just to make this AI more human...Otherwise when the ball gets in range, it might keep jumping
    //Thereby giving a very inhuman feel
    float nextChangeTime = 0f;
    //My states
    public static bool waiting, moving, jumping;
    #endregion


    void Start() {
        rigidBody = GetComponent<Rigidbody2D>();
        anim = GetComponent<Animator>();
        ball = GameObject.FindGameObjectWithTag("Ball");
        ballRigidbody = ball.GetComponent<Rigidbody2D>();
    }

    private void Update() {
        anim.SetFloat("Speed", Mathf.Abs(rigidBody.velocity.x));
        //Flip the character when necessary
        if ((ball.transform.position.x - transform.position.x) < 0 && !facingLeft) {
            Flip();
        }
        else if ((ball.transform.position.x - transform.position.x) > 0 && facingLeft) {
            Flip();
        }
    }

    void Flip() {
        facingLeft = !facingLeft;
        Vector3 scale = transform.localScale;
        scale.x *= -1;
        transform.localScale = scale;
    }





    private void FixedUpdate() {
        grounded = Physics2D.OverlapCircle(groundChecker.transform.position, groundCheckRadius, whatIsGround);

        if (GameController.playStarted && !BallBehaviour.connected) {           
            //Wait
            if (ball.transform.position.x < 0 && ballRigidbody.gravityScale == 0) {
                StartCoroutine(Wait());
            }
            //My serve
            else if (ball.transform.position.x > 0 && ballRigidbody.gravityScale == 0) {
                StartCoroutine(Serve());
            }
            //Jump
            else if (Mathf.Abs(ball.transform.position.x - transform.position.x) < 5 &&  Mathf.Abs(ball.transform.position.y - transform.position.y) < Random.Range(4,5) && Time.time > 0.3f + nextChangeTime) {
                //Jump every 0.3 seconds
                nextChangeTime = 0.3f + Time.time;
                StartCoroutine(Jump());
            }
            else if (Mathf.Abs(ball.transform.position.x - transform.position.x) < 5 && Mathf.Abs(ball.transform.position.x - transform.position.x) > 3 && Time.time > 0.3f + nextChangeTime) {
                //Jump every 0.3 seconds
                nextChangeTime = 0.3f + Time.time;
                StartCoroutine(Jump1());
            }

            //Move towards the predicted position
            if (Mathf.Sign(BallBehaviour.predictedPoint.x) == Mathf.Sign(transform.position.x) && BallBehaviour.impactPos.x > 0 && ballRigidbody.velocity.x < 10f && ballRigidbody.gravityScale != 0) {
                StartCoroutine(MoveTowardsPrediction(BallBehaviour.predictedPoint));
            }
            else if (Mathf.Sign(BallBehaviour.predictedPoint.x) == Mathf.Sign(transform.position.x)  && BallBehaviour.impactPos.x > 0 && ballRigidbody.velocity.x > 10f && BallBehaviour.predictedPoint.x > center.transform.position.x && ballRigidbody.gravityScale != 0) {
                BallBehaviour.predictedPoint.x = 8.87f - BallBehaviour.predictedPoint.x;
                StartCoroutine(MoveTowardsPrediction(BallBehaviour.predictedPoint));
            }
        }
    }

    private void OnCollisionEnter2D(Collision2D collision) {
        //Apply force to the ball so that it moves in the proper direction when hit by the player Controller
        if (collision.collider.CompareTag("Ball")) {
            ball.GetComponent<AudioSource>().Play();
            ballRigidbody.isKinematic = false;
            valueGiven = false;
            Vector2 diff = (ballRigidbody.transform.position - transform.position).normalized;
            //Ball is on the right
            if (diff.x >= 0) {
                //Find direction
                Vector2 difference = ball.transform.position - transform.position;
                float angle = Vector2.Angle(Vector2.right, difference);
                float randomAngle = Random.Range(angle, 90 - angle);
                Vector3 dir = Quaternion.AngleAxis(randomAngle, Vector3.forward) * Vector3.right;
                ballRigidbody.velocity = dir * ballForce;
            }
            else if (diff.x < 0) {
                Vector2 difference = ball.transform.position - transform.position;
                float angle = Vector2.Angle(Vector2.right, difference);
                float randomAngle = Random.Range(angle - 15, angle+ 15);
                //  print(randomAngle);
                Vector3 dir = Quaternion.AngleAxis(randomAngle, Vector3.forward) * Vector3.right;
                //rigidBody.AddForce(dir * ballForce);
                ballRigidbody.velocity = dir * ballForce;
            }
        }
    }

    IEnumerator Wait() {
        waiting = true;
        float value = Random.Range(-2f, 2f);
        float probabilityOfMoving = Random.value;
        if (probabilityOfMoving < 0.5f && !waiting) {
            rigidBody.velocity = new Vector2(value * moveSpeed, 0);
        }
        yield return null;
    }

    bool valueGiven = false;
    float saveValue;
    IEnumerator Serve() {
        Vector2 curDir = (ball.transform.position - transform.position).normalized;
        rigidBody.velocity = new Vector2(curDir.x * moveSpeed, curDir.y * jumpForce);
        print("In here");
        yield return null;
    }

    IEnumerator MoveTowardsPrediction(Vector3 target) {
        Vector3 newImpact = new Vector2(target.x + Random.Range(0, 0.75f), target.y);
        Vector2 x = (target - transform.position).normalized;
        rigidBody.velocity = new Vector2(x.x * moveSpeed, rigidBody.velocity.y);
        yield return null;
    }

    IEnumerator Jump() {
        if (grounded && jumpCount < maxJumpCount && !BallBehaviour.ballCollidedWithGround) {
          Vector2 jumpDirection = (ball.transform.position - transform.position).normalized;
            print("In jump");
            rigidBody.velocity = new Vector2(0, Random.Range(jumpForce - 2f,jumpForce + 4f));
        }
        else if (jumpCount >= maxJumpCount) {
            jumpCount = 0;
        }
        yield return null;
    }
    IEnumerator Jump1() {
        if (grounded && jumpCount < maxJumpCount && !BallBehaviour.ballCollidedWithGround) {
            Vector2 jumpDirection = (ball.transform.position - transform.position).normalized;
            print("In jump 2");
            rigidBody.velocity = new Vector2(moveSpeed * jumpDirection.x, Random.Range(jumpForce - 2f, jumpForce + 2f));
        }
        else if (jumpCount >= maxJumpCount) {
            jumpCount = 0;
        }
        yield return null;
    }



}