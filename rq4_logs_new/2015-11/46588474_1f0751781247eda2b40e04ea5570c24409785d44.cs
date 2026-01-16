using UnityEngine;
using System.Collections;
using System.Collections.Generic;

public class Memory : MonoBehaviour
{
    protected Hashtable statistics;     // Dictionnaire de stats
    [SerializeField]
    protected List<string> statNames;   // Liste des stats
    private int i;
    public static Memory instance;
    private bool isSecondDay = false;
    private bool isThirdDay = false;

    public static Memory Instance
    {
        get
        {
            return instance;
        }
    }

    // Use this for initialization
    void Start()
    {
        if (instance == null)
        {
            instance = this;
            DontDestroyOnLoad(this);
            statistics = new Hashtable();
            for (i = 0; i < statNames.Count; i++)
            {
                statistics[statNames[i]] = 0;
            }
        }
        else if (!isSecondDay)
            isSecondDay = true;
        else
            isThirdDay = true;
    }
    
    public Hashtable GetStatistics()
    {
        return statistics;
    }

    public List<string> GetStatNames()
    {
        return statNames;
    }

    public int GetDay()
    {
        if (!isSecondDay)
            return 1;
        else if (!isThirdDay)
            return 2;
        else
            return 3;
    }
}