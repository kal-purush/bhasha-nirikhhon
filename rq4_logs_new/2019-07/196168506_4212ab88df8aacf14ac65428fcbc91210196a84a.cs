using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class AttackButton:MonoBehaviour
{
    public int defender;
    public int attacker;
    public Manager mainManag;

    public void Start()
    {
        mainManag = GameObject.Find("EventSystem").GetComponent<Manager>();

    }

    public void Attack()
    {
        int defender;
        int attacker;
        attacker = gameObject.GetComponent<AttackButton>().attacker;
        mainManag.atkPnl.GetComponent<Image>().color = mainManag.playersInGame[attacker].pColor;
        mainManag.attackPanel.SetActive(true);
        defender = gameObject.GetComponent<AttackButton>().defender;
        mainManag.defPnl.GetComponent<Image>().color = mainManag.playersInGame[defender].pColor;

        mainManag.attacker = attacker;
        mainManag.defender = defender;
        mainManag.GenerateAtkPanel();
    }

}