using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class PlayerHUD : MonoBehaviour
{
    private Color defaultStaminaColor;
    private Color sprintStaminaColor;

    /* O player ao qual o HUD está associado */
    private Player localPlayer;

    private Slider staminaSlider;
    private Image staminaFill;

    void Start()
    {
        localPlayer = gameObject.GetComponent<Player>();

        staminaSlider = GameObject.FindGameObjectWithTag("StaminaSlider").GetComponent<Slider>();
        staminaFill = GameObject.FindGameObjectWithTag("StaminaFill").GetComponent<Image>();

        defaultStaminaColor = new Color(staminaFill.color.r, staminaFill.color.g, staminaFill.color.b, 1f); ;
        sprintStaminaColor = new Color(staminaFill.color.r, staminaFill.color.g, staminaFill.color.b, .5f); ;
    }

    void Update()
    {
        staminaSlider.value = localPlayer.vitals.stamina;
        staminaFill.color = localPlayer.movement.sprinting ? sprintStaminaColor : defaultStaminaColor;
    }

}