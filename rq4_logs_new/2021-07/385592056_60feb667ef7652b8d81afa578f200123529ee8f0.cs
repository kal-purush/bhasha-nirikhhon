using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class MoveController {
    private PlayerSM _playerSm;
    private CharacterController characterController;
    private float playerSpeed;
    private float gravityScale;
    private float jumpHeight;
    private float forceMultipler;
    private float friction;
    private float directionY;
    private float directionX;

    private float xBlast;
    private float yBlast;
    private Vector3 velocity;
    private bool isGrounded;
    private LayerMask mask;
    private Transform groundCheck;
    int xDirection;
    int yDirection;
    bool facingRight = true;

    public MoveController(){

    }

    public MoveController(PlayerSM playerSM, CharacterController controller, float speed, float gravityScale,float jumpHeight, float forceMultiplier, float friction, float xBlast, float yBlast, Transform groundCheck, LayerMask mask){
        this._playerSm = playerSM;
        this.characterController = controller;
        this.playerSpeed = speed;
        this.gravityScale = gravityScale;
        this.jumpHeight = jumpHeight;
        this.forceMultipler = forceMultiplier;
        this.friction = friction;
        this.xBlast = xBlast;
        this.yBlast = yBlast;
        this.groundCheck = groundCheck;
        this.mask = mask;
    }

    public void moveCharacter(){
        xDirection = (int)Input.GetAxisRaw("Horizontal");  
        yDirection = (int)Input.GetAxisRaw("Vertical");
        if(isGrounded){
            velocity = new Vector3();
            directionX = xDirection * playerSpeed;
            if(Input.GetButtonDown("Jump") && isGrounded){
                directionY = jumpHeight;
            }
        }
    }

    public void applyGravity(){
        
        isGrounded = UnityEngine.Physics.CheckSphere(groundCheck.position,0.1f,mask);
        if(!isGrounded && !Input.GetButton("Jump"))directionY += gravityScale * forceMultipler * Time.deltaTime; else if(!isGrounded) directionY += gravityScale * Time.deltaTime; else directionY = 0;
        Debug.Log("Gravity: " + directionY);
    }
    public void applyForces(){
       
        velocity.x = directionX;
        velocity.y = directionY;
        Debug.Log("APPLY FORCES: " + velocity.y);
        characterController.Move(velocity*Time.deltaTime);

    }
    public void slide(){
       
        Vector3 frictionVec = velocity;
        frictionVec.y = 0;
        frictionVec.Normalize();
        frictionVec *= -1;
        frictionVec *= friction;
        velocity += frictionVec;

    }

    public void Dash(){
    
        //reset the players momentum upon dash, if this isnt there and you rocket dash left, then rocket dash right,
        //because the forces are equal they cancel each other out and the player enters a stand still

        directionX = 0;
        directionY = 0;

        Debug.Log("BLAST: " + yBlast * -yDirection);

        directionX = xBlast * -xDirection;
        directionY = yBlast * yDirection;
    }
}