using UnityEngine;
using UnityEngine.UI;
using System.Collections;

public class GameData : MonoBehaviour
{
    //Declarations
    public bool P1Joined = false;
    public bool P2Joined = false;
    public bool P3Joined = false;
    public bool P4Joined = false;

    public Text P1Text;
    public Text P2Text;
    public Text P3Text;
    public Text P4Text;
    public Text PCount;

    public int players;

    // Use this for initialization
    void Start()
    {
        players = 0;
    }

    // Update is called once per frame
    void Update()
    {
        if (Application.loadedLevelName == "BasicTest1_Menu")
        {
            if (Input.GetButtonUp("P1Join") && P1Joined == false)
            {
                P1Joined = true;
                P1Text.text = "Joined";
                players++;
            }
            if (Input.GetButtonUp("P2Join") && P2Joined == false)
            {
                P2Joined = true;
                P2Text.text = "Joined";
                players++;
            }
            if (Input.GetButtonUp("P3Join") && P3Joined == false)
            {
                P3Joined = true;
                P3Text.text = "Joined";
                players++;
            }
            if (Input.GetButtonUp("P4Join") && P4Joined == false)
            {
                P4Joined = true;
                P4Text.text = "Joined";
                players++;
            }
            if (Input.GetButtonUp("P1voteP2") && P1Joined == true)
            {
                P1Joined = false;
                P1Text.text = "Left";
                players--;
            }
            if (Input.GetButtonUp("P2voteP2") && P2Joined == true)
            {
                P2Joined = false;
                P2Text.text = "Left";
                players--;
            }
            if (Input.GetButtonUp("P3voteP2") && P3Joined == true)
            {
                P3Joined = false;
                P3Text.text = "Left";
                players--;
            }
            if (Input.GetButtonUp("P4voteP2") && P4Joined == true)
            {
                P4Joined = false;
                P4Text.text = "Left";
                players--;
            }
            PCount.text = players.ToString();
        }
    }
}