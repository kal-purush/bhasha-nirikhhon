using System.Collections;
using UnityEngine;

public class CombatHud : MonoBehaviour
{
    public CombatEntityHud playerHud;
    public CombatEntityHud enemyHud;
    public GameObject buttonContainer;
    public TextAnimator combatMessage;

    private PlayerCombat playerUnit;
    private Enemy enemyUnit;

    public void Initialise(Enemy enemyUnit, PlayerCombat playerUnit)
    {
        this.enemyUnit = enemyUnit;
        this.playerUnit = playerUnit;

        playerHud.SetHUD(playerUnit);
        enemyHud.SetHUD(enemyUnit);
    }

    public void ShowPlayerCombatActions(bool show)
    {
        buttonContainer.SetActive(show);
    }

    public void UpdateHUD(string msg = "")
    {
        playerHud.SetHP(playerUnit.GetHealth(), playerUnit.GetMaxHealth());
        enemyHud.SetHP(enemyUnit.GetHealth(), enemyUnit.GetMaxHealth());

        if (!string.IsNullOrEmpty(msg) && combatMessage != null)
        {
            combatMessage.SetText(msg);
        }
    }

    public void UpdatePlayerHud(PlayerCombat player)
    {
        playerHud.SetHP(player.GetHealth(), player.GetMaxHealth());
    }

    public void UpdateEnemyHud(Enemy enemy)
    {
        enemyHud.SetHP(enemy.GetHealth(), enemy.GetMaxHealth());
    }
}