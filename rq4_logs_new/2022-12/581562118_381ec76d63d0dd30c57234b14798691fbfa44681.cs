using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class GameManager : MonoBehaviour
{
    public List<ScriptableCard> availableCards;

    [SerializeField] GameObject baseCard;
    [SerializeField] bool debugMode;

    GameObject newCard;

    void Start()
    {
        Application.targetFrameRate = 144; // TODO: create setting
        if (debugMode)
        {
            UnityEditor.EditorWindow.FocusWindowIfItsOpen(typeof(UnityEditor.SceneView));
        }

        ScriptableCard[] objects = Resources.LoadAll<ScriptableCard>("Decks/Blue");
        foreach (ScriptableCard obj in objects)
        {
            availableCards.Add(obj);
        }
    }

    private void Update()
    {
        if (Input.GetMouseButtonDown(0))
        {
            print("CARD");
            SpawnCard();
        }
    }

    public void SpawnCard()
    {
        if (newCard != null)
            Destroy(newCard);
        newCard = Instantiate(baseCard);
        newCard.transform.position = new Vector3(0,0,-9);
        newCard.GetComponent<Card>().scriptableCard = availableCards[Random.Range(0, availableCards.Count)];
    }
}