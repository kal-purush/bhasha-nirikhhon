using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class BubbleGum : MonoBehaviour
{

    float scaleRate = 0.001f;  //How fast the bubblegum fluctuates
    float deployRate =0.3f; //How fast the bubblegum deploys initially
    float minScale = 5.0f;
    float maxScale = 5.5f;

    bool gumActive = false;
    bool allowedToUseGum = true;

    int jumpCounter = 0;
    float gumGravity = -40f;
    float gumJumpHeight = 3f;



    
    public void Update() 
    {
        //if we exceed the defined range then correct the sign of scaleRate.
        if(transform.localScale.x < minScale) 
        {
            scaleRate = Mathf.Abs(scaleRate);
        }
        else if(transform.localScale.x > maxScale) 
        {
            scaleRate = -Mathf.Abs(scaleRate);
        }

        if(jumpCounter >= 5)
        {
            gumActive = false;
            allowedToUseGum = false;
            jumpCounter = 0;
        }

        if(Input.GetButtonDown("Fire1") && allowedToUseGum == true)  //Click the mouse to deploy bubblegum!
        {
            gumActive = true;
            CharController.velocity.y = Mathf.Sqrt(gumJumpHeight * -2f * gumGravity); //jump
            transform.localScale += (Vector3.one * deployRate);  //Quickly inflate bubble
            jumpCounter += 1;
        }

        if(gumActive == true)
        {
            transform.localScale += (Vector3.one * scaleRate);  //This applies the slow fluxuating size effect.
        }
        else
        {
            transform.localScale = new Vector3(0.0001f,0.0001f,0.0001f);
        }


        if ((allowedToUseGum == false) && CharController.isGrounded == true)
        {
            allowedToUseGum = true;
        }

    }
    







}