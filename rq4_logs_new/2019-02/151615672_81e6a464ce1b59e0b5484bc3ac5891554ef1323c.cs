using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class AbilityHandler : MonoBehaviour {

    //Public Editor Variables
    public GameObject magicMissile;
    public GameObject reflect;
    public GameObject absorb;
    public GameObject simulacrum;
    public Vector2 cursorInWorldPos;

    //Attack Variables
    private float atkSpeed = 135f;

    //Ability timers -- For the Attacks scripts
    private const float LONGATKCOOLDOWN = .2f;
    private const float HEALSTUNCOOLDOWN = .5f;
    private const float REFLECTCOOLDOWN = 3f;
    private const float ATKSIMCOOLDOWN = 2f;
    private const float ABSORBCOOLDOWN = 6f;
    private const float ABSORBSIMCOOLDOWN = 12f;
    private const float BURSTCOOLDOWN = 2f;

    private const float ABSORBEND = 3f;

    private float longATKTimer, healStunTimer, reflectTimer, 
    atkSimTimer, absorbTimer, absorbSimTimer, burstTimer;


    // Use this for initialization
    void Start () {
        longATKTimer = LONGATKCOOLDOWN;
        healStunTimer = HEALSTUNCOOLDOWN;
        reflectTimer = REFLECTCOOLDOWN;
        atkSimTimer = ATKSIMCOOLDOWN;
        absorbTimer = ABSORBCOOLDOWN;
        absorbSimTimer = ABSORBSIMCOOLDOWN;
        burstTimer = BURSTCOOLDOWN;

        StartCoroutine(LightUpdate());
    }

    private IEnumerator LightUpdate()
    {
        while(true)
        {
            BasicHandlers();
            yield return null;
        }
    }

    #region SpellFunctions

    public void AbilityChecker(int spell, bool isCombo, bool isBurst)
    {
        switch (isCombo)
        {
            case false:
                switch (spell)
                {
                    case 1:
                        MagicMissile();
                        break;
                    case 2:
                        Reflect();
                        break;
                    case 3:
                        HealStun();
                        break;
                }
                break;
            case true:
                switch (spell)
                {
                    case 7:
                        AttackSim(isBurst);
                        break;
                    case 8:
                        Absorb(isBurst);
                        break;
                    case 9:
                        AbsorbSim(isBurst);
                        break;
                }
                break;
        }
    }

    private void MagicMissile()
    {
        if(longATKTimer >= LONGATKCOOLDOWN)
        {
            //TODO Add MagicMissile Sound
            float tempX = 0; //Random.Range(-.15f, .15f);

            Vector2 direction = cursorInWorldPos - new Vector2(transform.position.x, transform.position.y);
            direction.Normalize();
            GameObject fb = Instantiate(magicMissile, transform.position, Quaternion.identity);
            fb.GetComponent<Rigidbody2D>().velocity = (direction + new Vector2(tempX, 0)) * atkSpeed;
            longATKTimer = 0;
        }
    }

    private void Reflect()
    {
        if(reflectTimer >= REFLECTCOOLDOWN)
        {
            //TODO Add Reflect Sound

            reflect.SetActive(true);
            reflectTimer = 0;
        }
    }

    private void HealStun()
    {
        //TODO Add HealStun Sound
    }

    //NewName - AtkSim
    private void AttackSim(bool isBurst)
    {
        if(isBurst)
        {
            //TODO Add AttackSim Burst Sound

            GameObject sim = Instantiate(simulacrum, transform.position, Quaternion.identity);
            sim.GetComponent<SimulacrumAbilities>().type = "Attack";
            sim.GetComponent<SimulacrumAbilities>().SetLifetime(4f);
        }
        else
        {
            //TODO Add AttackSim Ritual Sound

            GameObject sim = Instantiate(simulacrum, transform.position, Quaternion.identity);
            sim.GetComponent<SimulacrumAbilities>().type = "Attack";
            atkSimTimer = 0f;
        }
    }

    //NewName - Absorb
    private void Absorb(bool isBurst)
    {
        if(isBurst)
        {
            //TODO Add Absorb Burst Sound

            absorb.SetActive(true);
            absorbTimer = 0f;  
        }
        else
        {
            //TODO Add Absorb Ritual Sound

            absorb.SetActive(true);
            absorbTimer = 0f;  
        }
    }

    //NewName - ReflectSim
    private void AbsorbSim(bool isBurst)
    {
        if(isBurst)
        {
            //TODO Add AbsorbSim Burst Sound

            GameObject sim = Instantiate(simulacrum, transform.position, Quaternion.identity);
            sim.GetComponent<SimulacrumAbilities>().type = "Absorb";
        }
        else
        {
            //TODO Add AbsorbSim Ritual Sound

            GameObject sim = Instantiate(simulacrum, transform.position, Quaternion.identity);
            sim.GetComponent<SimulacrumAbilities>().type = "Absorb";
            absorbSimTimer = 0f;
        }
    }

    #endregion

    #region Handlers

    private void BasicHandlers()
    {
        TimerHandler();

        if (absorb.activeSelf && reflect.activeSelf)
            reflect.SetActive(false);

        cursorInWorldPos = Camera.main.ScreenToWorldPoint(Input.mousePosition);
    }

    private void TimerHandler()
    {
        longATKTimer += Time.deltaTime;
        healStunTimer += Time.deltaTime;
        reflectTimer += Time.deltaTime;
        atkSimTimer += Time.deltaTime;
        absorbTimer += Time.deltaTime;
        absorbSimTimer += Time.deltaTime;
        burstTimer += Time.deltaTime;

        if (reflectTimer >= 2f)
            reflect.SetActive(false);
        if(absorbTimer >= ABSORBEND)
        {
            absorb.SetActive(false);
        }
    }

    public float GetTimer(string str)
    {
        if (str == "attack")
            return longATKTimer;
        else if (str == "healstun")
            return healStunTimer;
        else if (str == "reflect")
            return reflectTimer;
        else if (str == "atkdash")
            return atkSimTimer;
        else if (str == "absorb")
            return absorbTimer;
        else
            return absorbSimTimer;
    }

    public float GetCooldown(string str)
    {
        if (str == "attack")
            return LONGATKCOOLDOWN;
        else if (str == "healstun")
            return HEALSTUNCOOLDOWN;
        else if (str == "reflect")
            return REFLECTCOOLDOWN;
        else if (str == "atkdash")
            return ATKSIMCOOLDOWN;
        else if (str == "absorb")
            return ABSORBCOOLDOWN;
        else
            return ABSORBSIMCOOLDOWN;
    }

    #endregion

}