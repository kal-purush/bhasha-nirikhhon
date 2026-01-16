using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;
using TMPro;
using UnityEngine.SceneManagement;

public class CombatManager : MonoBehaviour
{
    private PlayerHP playerHP;
    private bool playerTurn;
    public GameObject fightButton;
    public GameObject combatText;
    private int enemyHP;
    public string enemyName { get; set; }
    private Dictionary<string, string> combatTextDict;
    private string combatStep;

    // Start is called before the first frame update
    void Start()
    {
        playerHP = FindObjectOfType<PlayerHP>();
        playerTurn = true;
        enemyHP = 20;
        enemyName = "Scarab Enemy";
        fightButton.GetComponent<Button>().onClick.AddListener(FightEnemy);
        combatText.GetComponent<Button>().onClick.AddListener(AdvanceCombatStep);
        combatTextDict = new Dictionary<string, string>();
        combatTextDict.Add("playerAttack", "");
        combatTextDict.Add("enemyAttack", "");
        combatTextDict.Add("enemyDefeated", "");
        combatTextDict.Add("AfterBattle", "");
        combatStep = "playerAttack";
    }

    // Update is called once per frame
    void Update()
    {
        combatText.GetComponent<TMP_Text>().SetText(combatTextDict[combatStep]);
        if (playerTurn)
        {
            fightButton.SetActive(true);
            combatText.SetActive(false);
        }
        else
        {
            fightButton.SetActive(false);
            combatText.SetActive(true);
        }
    }
    private void FightEnemy()
    {
        int playerDamage = 10;
        // Play attack animation and enemy damage animation
        combatTextDict["playerAttack"] = $@"You attack {enemyName} and deal {playerDamage} damage.";
        enemyHP -= playerDamage;
        playerTurn = false;
    }
    private void AdvanceCombatStep()
    {
        Debug.Log(combatStep);
        if (combatStep == "playerAttack")
        {
            if (enemyHP <= 0)
            {
                // play enemy death animation
                combatTextDict["enemyDefeated"] = $@"You defeated {enemyName}!";
                combatStep = "enemyDefeated";
                return;
            }
            EnemyTurn();
        }
        else if (combatStep == "enemyAttack")
        {
            combatStep = "playerAttack";
            playerTurn = true;
        }
        else if (combatStep == "enemyDefeated")
        {
            combatTextDict["AfterBattle"] = "You gained 10 xp!";
            combatStep = "AfterBattle";
            StartCoroutine(ExitFight());
        }
    }
    private void EnemyTurn() // checks the current time and performs events at a certain time
    {
        int enemyDamage = 10;
        combatTextDict["enemyAttack"] = $@"The {enemyName} attacks you and deals {enemyDamage} damage.";
        combatStep = "enemyAttack";
        playerHP.damage(enemyDamage);
    }
    private IEnumerator ExitFight()
    {
        yield return new WaitForSeconds(3);
        SceneManager.LoadScene("Main World");
    }
}