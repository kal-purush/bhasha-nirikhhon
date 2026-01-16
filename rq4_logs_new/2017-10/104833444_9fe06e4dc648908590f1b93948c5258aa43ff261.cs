using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class FightUIController : MonoBehaviour {

    public Fighter player;
    public Fighter enemy;

    public Text playerHP;
    public Text playerStamina;

    private int oldPlayerHP;
    private int oldPlayerStamina;

    public Text enemyHP;

    private int oldEnemyHP;

	// Use this for initialization
	void Start () {
		
	}
	
	// Update is called once per frame
	void Update () {
        if (player.currHP != oldPlayerHP)
        {
            playerHP.text = player.currHP.ToString();
        }

        if (player.currStamina != oldPlayerStamina)
        {
            playerStamina.text = player.currStamina.ToString();
        }

        if (enemy.currHP != oldEnemyHP)
        {
            enemyHP.text = enemy.currHP.ToString();
        }


        oldPlayerHP = player.currHP;
        oldPlayerStamina = player.currStamina;
        oldEnemyHP = enemy.currHP;
	}
}