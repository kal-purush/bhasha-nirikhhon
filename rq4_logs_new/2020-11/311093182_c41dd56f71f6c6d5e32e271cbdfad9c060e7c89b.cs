using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;


public class HealthManager : MonoBehaviour
{

    Slider P1HPBar;
    Slider P2HPBar;
    [SerializeField] Gradient gradient;

    float MaxHP = 100;
    float currentP1HP;
    float currentP2HP;


    void Start()
    {
        currentP1HP = MaxHP;
        currentP2HP = MaxHP;

        P1HPBar = GameObject.FindGameObjectWithTag("Player1_HPBar").GetComponent<Slider>();
        P2HPBar = GameObject.FindGameObjectWithTag("Player2_HPBar").GetComponent<Slider>();
        SetHPBars();
    }

    // Update is called once per frame
    void Update()
    {
        
    }

    private void SetHPBars()
    {
        P1HPBar.maxValue = MaxHP;
        P1HPBar.value = currentP1HP;

        P2HPBar.maxValue = MaxHP;
        P2HPBar.value = currentP2HP;
    }

    public void ChangeHP(float damage, int player_id)
    {
        if(player_id == 0)
        {
            currentP2HP -= damage;
            P2HPBar.value = currentP2HP;
        }
        else if (player_id == 1)
        {
            currentP1HP -= damage;
            P1HPBar.value = currentP1HP;
        }
    }

    public bool checkEndGame()
    {
        if(currentP1HP <= 0 || currentP2HP <= 0)
        {
            return true;
        }
        return false;
    }

    public float GetCurrentP1HP()
    {
        return currentP1HP;
    }

    public float GetCurrentP2HP()
    {
        return currentP2HP;
    }
}