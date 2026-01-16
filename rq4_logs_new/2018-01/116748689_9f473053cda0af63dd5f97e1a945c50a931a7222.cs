using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class MP_Boost_Accel : MonoBehaviour {

    //works exactly like the script MP_Acceleration
    [SerializeField] float speed;
    bool boosting;
    bool canFill;
    //checking if the player is stopped
    bool stopped;

    [SerializeField] float maxBoostSpeed;
    //prevents acceleration from going past this number(set in the inspector)
    [SerializeField] float maxSpeed;
    float minSpeed = 0;
    GameObject PCont;
 
    [SerializeField] private float bar;
    private float bar_tofill = 30.0F;  //# of seconds it takes to fill bar
    private float bar_usage = 10.0F;   //No of seconds it can be used for
    float maxBar = 50f;

    [SerializeField] float OverheatTimer;

    MP_LaneValues LaneSwitch;
    MP_Overheat Heating;

    // Use this for initialization
    void Start()
    {
        PCont = GameObject.Find("PlayerContainer");
        
        bar = maxBar;
        LaneSwitch = GetComponent<MP_LaneValues>();
        Heating = GetComponent<MP_Overheat>();
    }

    // Update is called once per frame
    void Update()
    {
        if(bar>= maxBar)
        {
            canFill = false;
        }
    }

    void FixedUpdate()
    {

        if (!canFill)
        {
            bar -= bar_usage * Time.deltaTime;
            if (bar <= 0)
            {
                boosting = false;
                bar = maxBar;
                speed = minSpeed;
                LaneSwitch.OverheatTime();
                Heating.ChangeIsOverheated();

                //reset your speed here to what it was before

            }

        }
        else
        {
            if (bar < maxBar)
            {
                bar += bar_tofill * Time.deltaTime;
            }

        }

    }

    //controls acceleration, is called in the controls script
    public void Accelerate()
    {
        //print("Accelerate");
        //tells the script the player is no longer stopped
        stopped = false;
        //this moves the floor to the left, multiplied by speed which is constantly increased while button is held
        PCont.transform.Translate(Vector2.right * (speed += (Time.deltaTime * .15f)));

       
        //prevents speed from going too high
        if (speed > maxSpeed)
        {
            speed = maxSpeed;
        }

    }
    //controls deceleration, is called in the controls script
    public void Decelerate()
    {
        //checks if player is moving(not stopped)
        if (!stopped)
        {
            //slows the rate at which the floor is moved but still moves the floor to simulate deceleration
            PCont.transform.Translate(Vector2.right * (speed -= (Time.deltaTime * .1f)));

            //prevents speed from going negative
            if (speed < minSpeed)
            {
                speed = minSpeed;
                //tells script player is stopped when they hit 0 speed
                stopped = true;
            }

        }
    }

    public void Boost()
    {
        //print("Boost");
        boosting = true;
        
        //the only difference other than the stopped/boosting bool is this, it leaves Time.deltaTime normal, can be increased by multiplying Time.deltaTime;
        PCont.transform.Translate(Vector2.right * (speed += (Time.deltaTime * .15f)));
        
        if (speed > maxBoostSpeed)
        {
            speed = maxBoostSpeed;
        }
    }

    public void StopBoost()
    {
        canFill = true;
        if (boosting)
        {
            PCont.transform.Translate(Vector2.right * (speed -= Time.deltaTime * .1f));
            if(speed > maxSpeed)
            {
                speed = maxSpeed - .9f;
            }
          
            if (speed < minSpeed)
            {
                speed = minSpeed;
                boosting = false;
            }
        }
    }

    public void HalveSpeed()
    {
        speed = speed * 0.5f;
    }

    public void QuarterSpeed()
    {
        speed = speed * 0.25f;
    }

    public void ResetSpeed()
    {
        speed = 0;
    }

    public float GetSpeed()
    {
        return speed;
    }

    public bool GetStopped()
    {
        return stopped;
    }

    public float GetOverheatTime()
    {
        return OverheatTimer;
    }
}