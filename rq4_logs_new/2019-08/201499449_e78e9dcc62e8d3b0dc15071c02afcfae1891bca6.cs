using System.Collections;
using UnityEngine;
using UnityEngine.SceneManagement;
using UnityEngine.UI;

public class GoalManager : MonoBehaviour
{
    [SerializeField] private Office office;
    [SerializeField] private int days = 365;
    [SerializeField] private float secondsPerDay = 1f;
    [SerializeField] private Text remainingDaysText;

    public static float MoneyEarned { get; private set; }
    public static float EmployeesKilled { get; private set; }
    
    private int currentDay;
    
    private void Awake()
    {
        StartCoroutine(Countdown());
    }

    private IEnumerator Countdown()
    {
        currentDay = days;
        SetDaysLeftText(currentDay);
        while (currentDay > 0)
        {
            yield return new WaitForSeconds(secondsPerDay);
            currentDay--;
            SetDaysLeftText(currentDay);

            if (currentDay == 0)
            {
                // End game state
                OnGameEnd();
            }
        }
    }
    
    private void SetDaysLeftText(int daysLeft)
    {
        remainingDaysText.text = $"{daysLeft} days left";
    }

    private void OnGameEnd()
    {
        MoneyEarned = office.MoneyBalance;
        EmployeesKilled = 20; // TODO
        
        SceneManager.LoadScene("EndGameScene", LoadSceneMode.Single);
    }
}