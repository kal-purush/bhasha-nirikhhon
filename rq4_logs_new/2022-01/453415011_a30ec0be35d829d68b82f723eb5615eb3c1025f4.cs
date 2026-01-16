using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using Spine.Unity;

public class ItachiController : MonoBehaviour
{
    public SkeletonAnimation skeletonAnimation;
    public AnimationReferenceAsset idle, walking, jumping, running;
    public string currentState;
    public float speed;
    public float movement;
    private Rigidbody2D rigidbody;
    public string currentAnimation;
    public float jumpSpeed; 
    public string previousState; 


    // Start is called before the first frame update
    void Start()
    {
        rigidbody = GetComponent<Rigidbody2D>();
        currentState = "Idle";
        SetCharacterState(currentState);
    }

    // Update is called once per frame
    void Update()
    {
        Move();
    }
     public void SetAnimation(AnimationReferenceAsset animation, bool loop, float timeScale){
         if(animation.name.Equals(currentAnimation)){
             return;
         }
         Spine.TrackEntry animationEntry = skeletonAnimation.state.SetAnimation(0, animation, loop);
         animationEntry.TimeScale = timeScale; 
         animationEntry.Complete += AnimationEntry_Complete; 
         currentAnimation = animation.name; 
    }

    private void AnimationEntry_Complete(Spine.TrackEntry trackEntry){
        if (currentState.Equals("Jumping")){
            SetCharacterState(previousState);
        }
    }
    
    public void SetCharacterState(string state){

        if (state.Equals("Walking")){
            SetAnimation(walking, true, 0.8f);
        }
        else if (state.Equals("Jumping")){
            SetAnimation(jumping, false, 1.4f);
        }
        else if (state.Equals("Running")){
            SetAnimation(running, true, 1f);
        }
        else {
            SetAnimation(idle, true, 0.7f);
        } 

        currentState = state;
    }

    public void Move(){
        movement = Input.GetAxis("Horizontal");
        rigidbody.velocity = new Vector2(movement * speed, rigidbody.velocity.y);
        if (movement != 0){

            if(!currentState.Equals("Jumping")){
                SetCharacterState("Walking");
            }
            if (movement > 0){
                transform.localScale = new Vector2(1f, 1f);
            }
            else{
                transform.localScale = new Vector2(-1f, 1f);
            }
        }
        else {
            if(!currentState.Equals("Jumping")){
                 SetCharacterState("Idle");
            }
           
        }
        if(Input.GetButtonDown("Jump")){
            Jump();
        }
    }

    public void Jump(){
        rigidbody.velocity = new Vector2(rigidbody.velocity.x, jumpSpeed);
        if(!currentState.Equals("Jumping")){

            previousState = currentState;
        }
        SetCharacterState("Jumping");
    }

    public void Run(){
        
    }


}