using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class glider : playertest{

    void Update()
    {
        if (Input.GetKeyDown(KeyCode.M))
        {
            Application.LoadLevel("Jose's other scene");
        }
        anim.SetFloat("velocity", 0);
        bool touchingGround = checkGround();

        bounceDirection = new Vector2(Mathf.Lerp(bounceDirection.x, 0, Time.deltaTime * 2.5f), Mathf.Lerp(bounceDirection.y, 0, Time.deltaTime * (smashing ? 5 : 45)));

        if (!smashing && !vunrabilityFrames && playerControl != "" && rigid.bodyType == RigidbodyType2D.Dynamic)
        {
            if (Mathf.Abs(Input.GetAxis("Horizontal" + playerControl)) > 0.1f)
            {
                xSpeed += speed * Input.GetAxis("Horizontal" + playerControl) / (accelerate * 4);
                anim.GetComponent<SpriteRenderer>().flipX = Input.GetAxisRaw("Horizontal" + playerControl) < 0;
                anim.SetFloat("velocity", 1);
                slopeCheck();
            }
            else
            {
                xSpeed = Mathf.Lerp(xSpeed, 0, Time.deltaTime * decelerate);
            }

            xSpeed = Mathf.Clamp(xSpeed, -speed, speed);

            if (touchingGround)
            {
                transform.Translate(slidingFloor, 0, 0);
            }

            if (Input.GetButtonDown("Jump" + playerControl) && touchingGround)
            {
                rigid.velocity = new Vector2(rigid.velocity.x, maxJumpHeight);
                audioManager.instance.Play(jump, 0.5f, UnityEngine.Random.Range(0.93f, 1.05f));
            }

            if (Input.GetButtonUp("Jump" + playerControl) && rigid.velocity.y > minJumpHeight)
            {
                rigid.velocity = new Vector2(rigid.velocity.x, minJumpHeight);
            }

            if (Mathf.Abs(Input.GetAxis("Dash" + playerControl)) > 0.5f && canSmash && !touchingGround) {
                StartCoroutine(dashOutOfCharge(Input.GetAxis("Dash" + playerControl), true));
            }

            if (Input.GetButtonDown("Smash" + playerControl)
                    && canSmash && !smashing && !touchingGround)
            {
                StartCoroutine(chargeSmash(Input.GetAxis("Horizontal" + playerControl)));
            }
        }

        rigid.velocity += gravityDirection * 0.45f;

        if (!smashing && !vunrabilityFrames)
        {
            rigid.velocity = new Vector2(xSpeed, rigid.velocity.y) + bounceDirection;
        }
    }

    protected IEnumerator chargeSmash(float currentDirection) {
        bounceDirection = Vector2.zero;
        rigid.velocity = Vector2.right * rigid.velocity.x + bounceDirection;


        smashing = true;
        bool direction = GetComponent<SpriteRenderer>().flipX;
        float chargeValue = 0;
        SmashSpeed = minSmashSpeed;
        smashPower = minSmashPower;

        while (chargeValue <= maxChargeTime) {

            float lerp = (chargeValue / maxChargeTime);
            toggleCharge(lerp);

            SmashSpeed = Mathf.Lerp(minSmashSpeed, maxSmashSpeed, lerp);
            smashPower = Mathf.Lerp(minSmashPower, maxSmashPower, lerp);
            SmashCooldownTime = Mathf.Lerp(0.25f, maxSmashCooldownTime, lerp);

            if (!Input.GetButton("Smash" + playerControl)) {
                StartCoroutine(smashAfterCharge(chargeValue));
                yield break;
            }

            //Dash out of charge
            if (Mathf.Abs(Input.GetAxis("Dash" + playerControl)) > 0.5f && Input.GetAxis("Dash" + playerControl) != currentDirection) {
               StartCoroutine(dashOutOfCharge(chargeValue, direction));
               yield break;
            }

            currentDirection = Input.GetAxis("Dash" + playerControl);
            chargeValue += Time.deltaTime;
            rigid.velocity = (Vector2.right * rigid.velocity.x) * 0.95f + bounceDirection + Vector2.right * Input.GetAxis("Horizontal" + playerControl) * 0.2f;
            yield return new WaitForEndOfFrame();
        }
        StartCoroutine(smashAfterCharge(chargeValue));
    }
}