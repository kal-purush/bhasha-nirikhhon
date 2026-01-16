using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.EventSystems;
using UnityEngine.SceneManagement;
using UnityEngine.UI;

public class FlashcardFrenzyManager : MonoBehaviour
{
    private DatabaseManager databaseManager;

    public Button backButton;
    public Button nextFlashcard;
    public Button prevFlashcard;

    public GameObject flashcardTemplate;

    public Text flashcardSideText;
    public Text progressText;

    public Slider progressBar;
    private float progress;

    private int currentFlashcardIndex;
    private List<Flashcard> flashcards;
    private bool showingQuestion;

    private List<Flashcard> viewedFlashcards;

    // Start is called before the first frame update
    void Start()
    {
        viewedFlashcards = new List<Flashcard>();
        backButton.onClick.AddListener(Back);
        nextFlashcard.onClick.AddListener(NextFlashcard);
        prevFlashcard.onClick.AddListener(PreviousFlashcard);

        databaseManager = FindObjectOfType<DatabaseManager>();

        var topicId = PlayerPrefs.GetInt("TopicId");
        flashcards = databaseManager.ExecuteQueryWithReturn<Flashcard>("SELECT * FROM Flashcards WHERE TopicId = " + topicId + " ORDER BY RANDOM()");

        currentFlashcardIndex = 0;
        SwitchFlashcard();

        progress = 0f;
        UpdateProgressBar();

        // add event for when flashcard is clicked
        var eventTrigger = flashcardTemplate.GetComponent<EventTrigger>();
        EventTrigger.Entry entry = new EventTrigger.Entry();
        entry.eventID = EventTriggerType.PointerClick;
        entry.callback.AddListener((eventData) => { FlipFlashcard(); });
        eventTrigger.triggers.Add(entry);
    }

    private void FlipFlashcard()
    {
        var flashCardData = flashcards[currentFlashcardIndex];

        if (showingQuestion)
        {
            UpdateFlashcard(flashCardData.Answer);
            flashcardSideText.text = "ANSWER";
        }
        else
        {
            UpdateFlashcard(flashCardData.Question);
            flashcardSideText.text = "QUESTION";
        }

        showingQuestion = !showingQuestion;
    }

    private void NextFlashcard()
    {
        currentFlashcardIndex++;
        if(currentFlashcardIndex >= flashcards.Count)
        {
            currentFlashcardIndex = 0;
        }

        SwitchFlashcard();
    }

    private void PreviousFlashcard()
    {
        currentFlashcardIndex--;

        if(currentFlashcardIndex < 0)
        {
            currentFlashcardIndex = flashcards.Count - 1;
        }

        SwitchFlashcard();
        
    }

    private void SwitchFlashcard()
    {
        var flashcardData = flashcards[currentFlashcardIndex];

        UpdateFlashcard(flashcardData.Question);
        showingQuestion = true;
        flashcardSideText.text = "QUESTION";

        if (!viewedFlashcards.Contains(flashcardData))
        {
            UpdateProgressBar();
            viewedFlashcards.Add(flashcardData);
        }
    }

    private void Back()
    {
        SceneManager.LoadScene("Topic");
    }

    private void UpdateFlashcard(string flashcardText)
    {
        flashcardTemplate.GetComponentInChildren<Text>().text = flashcardText;
    }

    private void UpdateProgressBar()
    {
        progress++;
        progressBar.value = progress / (float)flashcards.Count;
        progressText.text = progress + " / " + flashcards.Count;
    }
}