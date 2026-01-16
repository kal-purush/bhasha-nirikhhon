using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class GameStatusController : MonoBehaviour{

    public static GameStatusController instance;

    public Text countdownText, remainingSlotsText;
    public string countdownTextPrefix = "Countdown: ";
    public string remainingTextPrefix = "Remaining Slots: ";

    // TODO: replace timer to somewhere else (level-controlled variable)
    public float timer = 30f;
    public int remainingSlots = 30;

    void Awake(){
        instance = instance ? instance : this;
        DontDestroyOnLoad(instance);
    }

    private void Update() {
        timer -= Time.deltaTime;
        countdownText.text = countdownTextPrefix + (int)timer;
        remainingSlotsText.text = remainingTextPrefix + remainingSlots;
    }
}