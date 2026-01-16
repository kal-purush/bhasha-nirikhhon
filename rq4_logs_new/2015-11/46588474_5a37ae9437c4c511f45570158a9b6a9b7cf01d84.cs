using UnityEngine;
using System.Collections;
using System.Collections.Generic;

public class GameManager : MonoBehaviour
{
    protected Hashtable statistics;     // Dictionnaire de stats
    [SerializeField]
    protected List<string> statNames;   // Liste des stats
    private int i;
    protected List<DayEvent> dayEvent;  // Liste des dayEvent à jouer jusqu'à l'appel de la scène Night suivante

    // Use this for initialization
    void Start()
    {
        statistics = new Hashtable();
        for (i = 0; i < statNames.Count; i++)
        {
            statistics[statNames[i]] = 0;
        }
    }

    // Update is called once per frame
    void Update()
    {

    }
}