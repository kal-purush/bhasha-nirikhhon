using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class FTUEManager : MonoBehaviour
{
    private static FTUEManager instance = null;
    public GameObject[] ftues;
    int ftueIdx = 0;

    public static FTUEManager Instance
    {
        get
        {
            if (instance == null)
            {
                Debug.Log("Game Manager is NULL");
            }
            return instance;
        }
    }

    private void Awake()
    {
        instance = this;
    }
    // Start is called before the first frame update
    void Start()
    {
        //Check to see if there is a ftue
        if (ftues.Length > 0)
        {
            ftues[0].gameObject.SetActive(true);
        }
    }

    public void NextFtue()
    {
        ftues[ftueIdx].gameObject.SetActive(false);
        ftueIdx++;
        if (ftueIdx < ftues.Length)
        {
            ftues[ftueIdx].gameObject.SetActive(true);
        }
    }
}