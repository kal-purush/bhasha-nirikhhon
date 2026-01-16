using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;



public enum TimeState { Morning, Afternoon, Night }

public class TimeController : MonoBehaviour
{
    public TimeState state;

    public GameObject playerPrefab;
    public GameObject timePrefab;

    public Text TimeText;

    // Start is called before the first frame update
    void Start()
    {
        state = TimeState.Morning;
        StartDay();

    }

    void StartDay()
    {
        GameObject playerGO = Instantiate(playerPrefab);
        GameObject timeGO = Instantiate(timePrefab);

        TimeText.text = "Morning";
        FreeRoam();
    }

    void FreeRoam()
    {

    }

}