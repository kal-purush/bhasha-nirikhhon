using UnityEngine;
using System.Collections;

public class UserInputFalconRPhysics : MonoBehaviour {

    public float diveTransitionConstant;
    public float diveTransitionYHeightConstant;
    public float diveDownTransitionTime;
    public float diveToBoostTransitionTime;
    public float boostYHeightConstant;
    public float boostXShiftConstant;
    public float boostEnergyMultiplier;
    public float boostYSteepness;
    public float boostMultiplier;

    falconStates currentState;
    float timeHeld;
    float timeGlide;
    float timeBoost;
    float timeDiveToBoostTransition;
    float boostEnergy;
    float boostEnergyTime;
    float boostAirTime;
    bool diveToBoostTransitionDone = false;
    Vector2 tempVect = new Vector2(1, -1);
    
    public enum falconStates
    {
        gliding,
        boosting,
        diving,
    }

	// Use this for initialization
	void Start () {
        currentState = falconStates.gliding;
	}

    // y = 3(x^2) + 4
    float expDiveEquation(float timeDiveHeld)
    {
        return diveTransitionConstant * Mathf.Pow(timeDiveHeld, 2) + diveTransitionYHeightConstant;
    }

    // f'(x) = n * 2x
    float expDiveDerivative(float timeDiveHeld)
    {
        return diveTransitionConstant * 2 * timeDiveHeld;
    }

    float boostEquation(float timeBoosting)
    {
        float secondXIntercept = (boostYSteepness * boostAirTime * 2) / Mathf.Pow(boostAirTime, 2);
        Debug.Log("SecondX: " + secondXIntercept);
        return (-Mathf.Pow((secondXIntercept * timeBoosting - boostYSteepness), 2) + Mathf.Pow(boostYSteepness, 2)) * boostMultiplier * (1/secondXIntercept);
    }

    float linearDiveEquation (float timeDiveHeld)
    {
        float slope = expDiveDerivative(diveDownTransitionTime);
        float yIntercept = expDiveEquation(diveDownTransitionTime);
        return slope * (timeDiveHeld - diveDownTransitionTime) + yIntercept;
    }

    //     1
    //-----------
    // 1 + e^(-x)
    float sigmoidBoostEquation (float timeBoosting)
    {
        return boostYHeightConstant / (1 + Mathf.Exp(-timeBoosting + boostXShiftConstant));
    }
	
	// Update is called once per frame
	void Update () {
        if (Input.GetKeyDown(KeyCode.D))
        {
            currentState = falconStates.diving;
        }
        else if (Input.GetKeyUp(KeyCode.D))
        {
            currentState = falconStates.boosting;
        }


        if (currentState == falconStates.gliding)
        {
            float tempY = 0;
            timeGlide += Time.deltaTime;

            //tempY = .1f * Mathf.Pow(timeGlide, 2);
            tempY = timeGlide;

            timeHeld = 0.0f;
            tempVect = new Vector2(1, -tempY);
        }
        else if (currentState == falconStates.boosting)
        {
            float tempY = 0;
            timeBoost += Time.deltaTime;

            //If no more boost energy, return to gliding
            if (boostEnergy <= 0)
            {
                currentState = falconStates.gliding;
            }
            //Transition from diving to boosting
            else if (timeBoost < diveToBoostTransitionTime && !diveToBoostTransitionDone)
            {
                tempY = expDiveEquation(boostEnergyTime);
            }
            else if (!diveToBoostTransitionDone)
            {
                diveToBoostTransitionDone = true;
                timeBoost = 0.0f;
                boostAirTime = boostEnergy;
            }
            //Boosting
            else
            {
                //tempY = -sigmoidBoostEquation(timeBoost);
                //tempY = -.1f * timeBoost;
                tempY = -boostEquation(timeBoost);
                Debug.Log(tempY);
            }
            boostEnergy -= Time.deltaTime;
            boostEnergyTime -= Time.deltaTime;
            timeHeld = 0.0f;
            tempVect = new Vector2(1, -tempY);
        }
        else if (currentState == falconStates.diving)
        {
            float tempY = 0;
            timeHeld += Time.deltaTime;
            boostEnergy = boostEnergyMultiplier * timeHeld; //Convert time diving into boost power
            boostEnergyTime = timeHeld;
            timeGlide = 0.0f;
            timeBoost = 0.0f;
            diveToBoostTransitionDone = false;

            //Initial Drop of Falcon
            if (timeHeld < diveDownTransitionTime)
            {
                tempY = expDiveEquation(timeHeld);
            }
            //Linear dive
            else
            {
                tempY = linearDiveEquation(timeHeld);
            }
            tempVect = new Vector2(1, -tempY);
        }

        transform.Translate(tempVect * Time.deltaTime, Space.World);
	}
}