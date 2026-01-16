using System;
using System.Collections;
using System.Collections.Generic;
using TMPro;
using UnityEngine;
using UnityEngine.SceneManagement;
using UnityEngine.UI;

public class BossFight : MonoBehaviour
{
    [SerializeField] private UI ui;
    [Header("Player")]
    [SerializeField] private Slider playerHealthBar;
    [SerializeField] private GameObject playerBubbleAnchor;
    [SerializeField] private TextMeshProUGUI playerBubbleText;
    [SerializeField] private RectTransform playerImage;

    [Header("Grinchelbart")]
    [SerializeField] private Slider grinchelbartHealthBar;
    [SerializeField] private GameObject grinchelbartBubbleAnchor;
    [SerializeField] private TextMeshProUGUI grinchelbartBubbleText;
    [SerializeField] private RectTransform grinchelbartImage;

    [Header("Selection")]
    [SerializeField] private GameObject answers;
    [SerializeField] private GameObject fights;

    [Header("Aufgaben")]
    [SerializeField] private BossAufgaben[] aufgaben;

    [Header("Fight")]
    [SerializeField] private int damageTakenBlock = 5;
    [SerializeField] private int damageTakenDodgeMiss = 10;
    [SerializeField] private int damageDealtDodgeSuccess = 5;
    [SerializeField] private int damagePunchSuccess = 24;
    [SerializeField] private int damagePunchMiss = 17;
    [SerializeField] private int damageKickSuccess = 25;
    [SerializeField] private int damageKickMiss = 12;

    [Header("Endscreen")]
    [SerializeField] private GameObject endscreen;
    [SerializeField] private TextMeshProUGUI endscreenText;
    [SerializeField] private Animator endscreenAnimator;
    [SerializeField] private string[] endscreenTextWin;
    [SerializeField] private string[] endscreenTextLose;
    [SerializeField] private float delay = 1f;

    private int[] randomOrder;
    private int currentAufgabe = 0;
    private bool fightWithDisadvantage = false;

    [System.Serializable]
    private class BossAufgaben
    {
        public string aufgabe;
        public string[] antworten;
        public int richtigeAntwort; // 0-3
    }

    private void Start()
    {
        ui.HideButtons();
        randomOrder = new int[aufgaben.Length];
        for (int i = 0; i < randomOrder.Length; i++)
        {
            randomOrder[i] = i;
        }
        randomOrder = Shuffle(randomOrder);
        Aufgabe();
    }

    private int[] Shuffle(int[] randomOrder)
    {
        for (int i = 0; i < randomOrder.Length; i++)
        {
            int temp = randomOrder[i];
            int randomIndex = UnityEngine.Random.Range(i, randomOrder.Length);
            randomOrder[i] = randomOrder[randomIndex];
            randomOrder[randomIndex] = temp;
        }
        return randomOrder;
    }

    public void Aufgabe()
    {
        if (currentAufgabe >= aufgaben.Length)
        {
            // Reshuffle
            randomOrder = Shuffle(randomOrder);
            currentAufgabe = 0;
        }
        BossAufgaben aufgabe = aufgaben[randomOrder[currentAufgabe]];
        answers.SetActive(true);
        fights.SetActive(false);
        grinchelbartBubbleText.text = aufgabe.aufgabe;
        grinchelbartBubbleAnchor.SetActive(true);
        playerBubbleAnchor.SetActive(false);
        playerBubbleText.text = "";

        // Set answers
        for (int i = 0; i < aufgabe.antworten.Length; i++)
        {
            answers.transform.GetChild(i).GetComponentInChildren<TextMeshProUGUI>().text = aufgabe.antworten[i];
        }
    }

    public void CheckAnswer(int index)
    {
        BossAufgaben aufgabe = aufgaben[randomOrder[currentAufgabe]];

        playerBubbleAnchor.SetActive(true);
        playerBubbleText.text = aufgabe.antworten[index];

        if (aufgabe.richtigeAntwort == index)
        {
            // Richtige Antwort
            var antwortText = new string[] { "Nein! Das darf nicht sein!", "Das ist nicht möglich!", "Du bist zu gut!" };
            grinchelbartBubbleText.text = antwortText[UnityEngine.Random.Range(0, antwortText.Length)];
            grinchelbartBubbleAnchor.SetActive(true);
            currentAufgabe++;
            answers.SetActive(false);
            fights.SetActive(true);
            fightWithDisadvantage = false;
        }
        else
        {
            // Falsche Antwort
            var antwortText = new string[] { "Hahahah! Du wirst mich nie aufhalten!", "Leider falsch!", "Niemand wird mich aufhalten!" };
            grinchelbartBubbleText.text = antwortText[UnityEngine.Random.Range(0, antwortText.Length)];
            grinchelbartBubbleAnchor.SetActive(true);
            currentAufgabe++;
            answers.SetActive(false);
            fights.SetActive(true);
            fightWithDisadvantage = true;
        }
        foreach (Button button in fights.GetComponentsInChildren<Button>())
        {
            button.interactable = true;
        }
    }

    public void Fight(int index)
    {
        foreach(Button button in fights.GetComponentsInChildren<Button>())
        {
            button.interactable = false;
        }

        playerBubbleAnchor.SetActive(false);
        grinchelbartBubbleAnchor.SetActive(false);
        switch (index)
        {
            case 0:
                // Block
                playerHealthBar.value -= damageTakenBlock;
                break;
            case 1:
                // Dodge
                if (UnityEngine.Random.Range(0, 4) == 0)
                {
                    // failed
                    playerHealthBar.value -= damageTakenDodgeMiss;
                }
                else
                {
                    // success
                    grinchelbartHealthBar.value -= damageDealtDodgeSuccess;
                }
                break;
            case 2:
                // Punch
                grinchelbartHealthBar.value -= fightWithDisadvantage ? damagePunchMiss : damagePunchSuccess;
                playerHealthBar.value -= fightWithDisadvantage ? damagePunchSuccess : damagePunchMiss;
                break;
            case 3:
                // Kick
                grinchelbartHealthBar.value -= fightWithDisadvantage ? damageKickMiss : damageKickSuccess;
                playerHealthBar.value -= fightWithDisadvantage ? damageKickSuccess : damageKickMiss;
                break;
        }

        StartCoroutine(FightAnimation(index));
    }

    private IEnumerator FightAnimation(int index)
    {
        float elapsedTime = 0f;
        float duration = 0.5f;
        Vector2 start = playerImage.localPosition;
        Vector2 startGrinchelbart = grinchelbartImage.localPosition;
        Vector2 end = new Vector2(start.x, start.y - 50);
        Vector2 endGrinchelbart = new Vector2(startGrinchelbart.x + 600, startGrinchelbart.y - 150);
        // Animation
        switch (index)
        {
            case 0:
                // Block
                end = new Vector2(start.x, start.y - 50);
                endGrinchelbart = new Vector2(startGrinchelbart.x + 600, startGrinchelbart.y - 150);
                break;
            case 1:
                // Dodge
                end = new Vector2(start.x + 350, start.y + 100);
                endGrinchelbart = new Vector2(startGrinchelbart.x + 600, startGrinchelbart.y - 150);

                break;
            case 2:
                // Punch
                end = new Vector2(start.x - 300, start.y + 150);
                endGrinchelbart = new Vector2(startGrinchelbart.x + 290, startGrinchelbart.y - 75);
                break;
            case 3:
                // Kick
                end = new Vector2(start.x + 50, start.y - 50);
                endGrinchelbart = new Vector2(startGrinchelbart.x - 25, startGrinchelbart.y + 25);
                break;
        }
        while (elapsedTime < duration)
        {
            playerImage.localPosition = Vector2.Lerp(start, end, elapsedTime / duration);
            grinchelbartImage.localPosition = Vector2.Lerp(startGrinchelbart, endGrinchelbart, elapsedTime / duration);
            elapsedTime += Time.deltaTime;
            yield return null;
        }
        // reset
        elapsedTime = 0f;
        duration = 1f;
        while (elapsedTime < duration)
        {
            playerImage.localPosition = Vector2.Lerp(end, start, elapsedTime / duration);
            grinchelbartImage.localPosition = Vector2.Lerp(endGrinchelbart, startGrinchelbart, elapsedTime / duration);
            elapsedTime += Time.deltaTime;
            yield return null;
        }

        // Proceed
        if (grinchelbartHealthBar.value <= 0)
        {
            // Win
            StartCoroutine(Endscreen(true));
        }
        else if (playerHealthBar.value <= 0)
        {
            // Lose
            StartCoroutine(Endscreen(false));
        }
        else
        {
            // Continue
            Aufgabe();
        }
        yield return null;
    }

    public IEnumerator Endscreen(bool win = false)
    {
        endscreen.SetActive(true);
        yield return new WaitForSeconds(delay);
        if (win)
        {
            StartCoroutine(Fade(endscreenTextWin));
        }
        else
        {
            StartCoroutine(Fade(endscreenTextLose));
            StartCoroutine(ReloadScene());
        }
    }

    public IEnumerator ReloadScene()
    {
        yield return new WaitForSeconds(delay * (endscreenTextLose.Length - 1) * 3 + delay);
        SceneManager.LoadScene(SceneManager.GetActiveScene().buildIndex);
    }

    private IEnumerator Fade(string[] strings)
    {
        if (endscreenText.alpha == 0) endscreenText.text = strings[0];

        float duration = delay;
        float currentTime = 0f;
        float start = endscreenText.alpha;
        float end = start == 0 ? 1 : 0;

        while (currentTime < duration)
        {
            float alpha = Mathf.Lerp(start, end, currentTime / duration);
            endscreenText.alpha = alpha;
            currentTime += Time.deltaTime;
            yield return null;
        }

        endscreenText.alpha = end;

        yield return new WaitForSeconds(delay);

        if (end == 1)
        {
            if (strings.Length > 1)
            {
                StartCoroutine(Fade(strings[1..]));
            }
        }
        else
        {
            StartCoroutine(Fade(strings));
        }
    }
}