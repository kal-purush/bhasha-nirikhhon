using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class PlayerDialogue : MonoBehaviour
{
    List<string> NPCs = new List<string>();
    Dictionary<int, string> PlayerGeneralQuestions = new Dictionary<int, string>();

    Dictionary<int, string> KalushiQuestions = new Dictionary<int, string>();
    Dictionary<int, string> BandileQuestions = new Dictionary<int, string>();
    Dictionary<int, string> JamesQuestions = new Dictionary<int, string>();
    Dictionary<int, string> ThandiQuestions = new Dictionary<int, string>();
    Dictionary<int, string> RoliQuestions = new Dictionary<int, string>();

    // Start is called before the first frame update
    void Start()
    {
        NPCs.Add("James");
        NPCs.Add("Thandi");
        NPCs.Add("Kali");
        NPCs.Add("Roli");
        NPCs.Add("Bandile");

        PlayerGeneralQuestions.Add(1, "What made you want to join Umkhonte we Sizwe?");
        PlayerGeneralQuestions.Add(2, "What do you think about "); //choice of names exclusing player/person talking to
        PlayerGeneralQuestions.Add(3, "Are you more of a pacifist or a warmonger?");

        KalushiQuestions.Add(1, "Why was your sentence shortened?");
        KalushiQuestions.Add(2, "Are you more of a pacifist or a warmonger?");

        BandileQuestions.Add(1, "What is your full name?");
        BandileQuestions.Add(2, "WHy did you join the ANC and not the PAC?");

        JamesQuestions.Add(1, "If you're a pacifisit, why did you join the militiary wing, Umkhonte we Sizwe");
        JamesQuestions.Add(2, "What was it like growing up in a small rural town");

        ThandiQuestions.Add(2, "What happened to the other glove?"); //check to see it this object has been interacted with
        ThandiQuestions.Add(1, "How do you not see black officers as traitors?");
    }

    // Update is called once per frame
    void Update()
    {
        
    }
}