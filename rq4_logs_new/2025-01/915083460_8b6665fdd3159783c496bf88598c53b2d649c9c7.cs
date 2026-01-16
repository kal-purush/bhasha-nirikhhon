using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using Spine.Unity;

public class PlayerController : MonoBehaviour
{
    //
    private float horizontalInput;
    private int jumpCount;
    [SerializeField] private int setUpJumpCount = 2;
    [SerializeField] private float moveSpeed = 1f;
    [SerializeField] private float jumpPower = 1f;
    [SerializeField] private float setUpGravity = 10f; 
    [SerializeField] private LayerMask groundLayer;
    [SerializeField] private LayerMask wallLayer;
    [SerializeField, ReadOnly] private float wallJumpCoolDown; 
    [SerializeField, ReadOnly] private Rigidbody2D rb;
    [SerializeField, ReadOnly] private CapsuleCollider2D capsuleCollider;
    [SerializeField, ReadOnly] private SkeletonAnimation playerAnimation;
    

#region Unity Method

    private void Awake(){
        rb = GetComponent<Rigidbody2D>();
        capsuleCollider = GetComponent<CapsuleCollider2D>();
        playerAnimation = GetComponentInChildren<SkeletonAnimation>();
    }

    private void Start(){
        rb.gravityScale = setUpGravity;
        jumpCount = setUpJumpCount;
    }

    private void Update(){
        CheckJumpCount();
        horizontalInput = Input.GetAxis("Horizontal");

        // Flip player
        if (horizontalInput > 0.01f) 
            transform.localScale = Vector3.one;
        else if (horizontalInput < -0.01f)
            transform.localScale = new Vector3 (-1, 1, 1);

        if (wallJumpCoolDown < 0.2f ) {
            rb.velocity = new Vector2(horizontalInput * moveSpeed, rb.velocity.y);

            if (onWall() && !isGrounded()){
                rb.velocity = new Vector2(rb.velocity.x, Mathf.Max(-2f, rb.velocity.y)); // Giảm tốc độ rơi nhẹ
            } else
                rb.gravityScale = setUpGravity;

            if(Input.GetKeyDown(KeyCode.Space) && jumpCount > 0){
                Jump();
                wallJumpCoolDown = 0;
            }
        } else {
            wallJumpCoolDown += Time.deltaTime;
        }

        print(jumpCount);
        
        if (isGrounded()) {
            if (Mathf.Abs(horizontalInput) > 0.01f) {
                PlayAnimation(0, "Run", true); // Đang chạy
            } else {
                PlayAnimation(0, "Idle", true); // Đứng yên
            }
        } else if (rb.velocity.y > 0.1f) {
            PlayAnimation(0, "Jump", false); // Đang nhảy lên
        } else if (rb.velocity.y < -0.1f) {
            PlayAnimation(0, "Fall", false);
        } else if (onWall()){
            PlayAnimation(0, "Climb", true);
        }
    }
#endregion

#region Jump Logic
    private void Jump(){
        if (isGrounded() || jumpCount > 0){
            rb.velocity = new Vector2(rb.velocity.x, jumpPower);
            jumpCount--;
        } else if(onWall() && !isGrounded()){
            if (horizontalInput == 0){
                rb.velocity = new Vector2(-Mathf.Sign(transform.localScale.x) * 25, 0);
                transform.localScale = new Vector3(-MathF.Sign(transform.localScale.x), transform.localScale.y, transform.localScale.z);
            } else {
                rb.velocity = new Vector2(-Mathf.Sign(transform.localScale.x) * 3, 25);
            }
            wallJumpCoolDown = 0;
        }     
    }

    private void CheckJumpCount(){
        if (isGrounded() || onWall()){
            jumpCount = setUpJumpCount;
        }
    }
    // private void OnCollisionEnter2D (Collision2D collision){

    // }

    private bool isGrounded(){
        RaycastHit2D raycastHit = Physics2D.BoxCast(capsuleCollider.bounds.center, capsuleCollider.bounds.size, 0, Vector2.down, 0.1f, groundLayer);
        return raycastHit.collider != null;
    }

    private bool onWall(){
        RaycastHit2D raycastHit = Physics2D.BoxCast(capsuleCollider.bounds.center, capsuleCollider.bounds.size, 0, new Vector2(transform.localScale.x, 0), 0.2f, wallLayer);
        return raycastHit.collider != null;
    }

#endregion

#region Spine Controller
    private void PlayAnimation(int track, string name, bool loop){
        if (playerAnimation.AnimationName != name){
            playerAnimation.state.SetAnimation(track, name, loop);
        }
    }

    private void ClearTrack(int track){
        playerAnimation.state.ClearTrack(track);
    }

#endregion
}