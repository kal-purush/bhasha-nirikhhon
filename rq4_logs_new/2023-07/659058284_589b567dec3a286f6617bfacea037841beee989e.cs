using UnityEngine;

public class BalanceGame : MonoBehaviour
{
    public Transform tChart; // Reference to the t-chart object in the scene
    public GameObject starPrefab; // Reference to the star prefab

    public float roundTime = 10f; // Time for each round
    public int numRounds = 4; // Number of rounds
    public float difficultyIncrease = 0.2f; // Difficulty increase for each round

    private int currentRound = 1;
    private int starsToRemove;
    private float remainingTime;
    private bool isGameOver;

    private void Start()
    {
        StartNewRound();
    }

    private void Update()
    {
        if (!isGameOver)
        {
            remainingTime -= Time.deltaTime;

            if (remainingTime <= 0f)
            {
                isGameOver = true;
                Debug.Log("Time's up! Game Over!");
            }
        }
    }

    private void StartNewRound()
{
    // Calculate the number of stars to remove in this round
    starsToRemove = Mathf.RoundToInt(tChart.childCount / 2f);

    // Remove previous stars
    foreach (Transform star in tChart)
    {
        Destroy(star.gameObject);
    }

    // Create new stars
    for (int i = 0; i < starsToRemove * 2; i++)
    {
        InstantiateStar();
    }

    // Reset timer
    remainingTime = roundTime;

    // Update UI or display relevant information to the player
    // ... (Implement your UI updates or feedback to the player)
}


    private void InstantiateStar()
    {
        GameObject star = Instantiate(starPrefab, tChart);
        star.transform.localPosition = new Vector3(Random.Range(-2f, 2f), Random.Range(-2f, 2f), 0f);
        star.GetComponent<bStar>().balanceGame = this;
    }

    public void RemoveStar(GameObject star)
    {
        if (isGameOver)
            return;

        Destroy(star);
        starsToRemove--;

        if (starsToRemove == 0)
        {
            if (currentRound == numRounds)
            {
                isGameOver = true;
                Debug.Log("Congratulations! You completed all rounds!");
                // Display victory screen or perform game over logic
            }
            else
            {
                currentRound++;
                roundTime -= difficultyIncrease;
                StartNewRound();
            }
        }
    }
}





