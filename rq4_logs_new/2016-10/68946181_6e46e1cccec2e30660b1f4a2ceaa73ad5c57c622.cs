using UnityEngine;
using System.Collections;

/// <summary>
/// Quest Dialogue script.
/// Used to create a simple quest system.
/// </summary>
public class QuestDialogue : MonoBehaviour {
    public Texture2D window;
    public Texture2D mouseIcon;
    private Vector2 pos = new Vector2(400, 600);
    private Vector2 size = new Vector2(800, 200);
    public string[] questions;
    public string[] answers;
    private bool PlayerInTalkingRange = false;
    private bool displayDialogue = false;
    private bool questActive = false;
    public bool questComplete = false;
    private bool questRewardGiven = false;
    public int RewardAmount;

    private int margin = 10;
    public GUIStyle textGUI = new GUIStyle();
    public GUIStyle buttonGUI = new GUIStyle();

    //public GameObject item;
    //public Vector2 itemPOS = new Vector2(0, 0);

    // START:
    void Start () {
        //Instantiate(item, new Vector3(itemPOS.x, itemPOS.y, 0), Quaternion.identity);
    }


    // UPDATE:
    void Update() {
        if (Input.GetMouseButtonDown(1) && PlayerInTalkingRange) {
            Debug.Log("Player talked to NPC.");
            displayDialogue = true;
        }
    }


    // OnGUI:
    void OnGUI() {
        if (PlayerInTalkingRange && !displayDialogue) {
            GUI.DrawTexture(new Rect((Screen.width / 2) + 32, (Screen.height / 2) - 64, 16, 32), mouseIcon, ScaleMode.StretchToFill, true, 1);
        }

        GUILayout.BeginArea(new Rect(pos.x, pos.y, size.x, size.y));
        if (displayDialogue) {
            GUI.DrawTexture(new Rect(0, 0, size.x, size.y), window, ScaleMode.StretchToFill, true, 1);
        }        
            GUILayout.BeginArea(new Rect(margin, margin, size.x - (margin * 2), size.y - (margin * 2)));
                if (displayDialogue && !questActive && !questComplete) {
                    GUILayout.Label(questions[0], textGUI); // I have a quest for you.
                    GUILayout.Label(questions[1], textGUI); // Will you do the quest?

                    if (GUILayout.Button(answers[0], buttonGUI)) { // Accept.
                        questActive = true;                
                        displayDialogue = false;
                    }

                    if (GUILayout.Button(answers[1], buttonGUI)) { // Decline.
                        displayDialogue = false;
                    }
                }

                else if (displayDialogue && questActive && !questComplete) {
                    GUILayout.Label(questions[2], textGUI); // Do the quest!

                    if (GUILayout.Button(answers[2], buttonGUI)) { // Okay.
                        //questComplete = true;
                        displayDialogue = false;
                    }
                }

                else if (displayDialogue && questComplete && !questRewardGiven) {
                    GUILayout.Label(questions[3], textGUI); // You did the quest. Here is your reward.

                    if (GUILayout.Button(answers[3], buttonGUI)) { // Thanks.
                        QuestComplete();
                        questRewardGiven = true;
                        displayDialogue = false;
                    }
                }

                else if (displayDialogue && questRewardGiven) {
                    GUILayout.Label(questions[4], textGUI); // I have nothing more to say.

                    if (GUILayout.Button(answers[4], buttonGUI)) { // Goodbye.
                        displayDialogue = false;
                    }
                }
            GUILayout.EndArea();
        GUILayout.EndArea();
    } // END OnGUI


    /// ON TRIGGER ENTER (2D):
    void OnTriggerEnter2D(Collider2D col) {
        if (col.gameObject.tag == "Player") {
            PlayerInTalkingRange = true;
        }
    }


    // ON TRIGGER EXIT (2D):
    void OnTriggerExit2D() {
        PlayerInTalkingRange = false;
        displayDialogue = false;
    }


    // QUEST COMPLETE:
    void QuestComplete() {
        //PlayerGold.playerGold += RewardAmount;
        //DisableQuestCollider();
    }

    // DISABLE QUEST COLLIDER:
    void DisableQuestCollider() {
        this.GetComponent<Collider2D>().enabled = false;
    }
}