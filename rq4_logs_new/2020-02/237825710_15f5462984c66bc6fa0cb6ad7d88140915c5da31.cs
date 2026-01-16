using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class MainMenuAssignments : MonoBehaviour {

    public Text infoText;
    public Sprite[] spritesToChooseFrom;
    public KeyCode[] keyCodes = new KeyCode[4];

    int currentPlayer = 0;
    bool assigningKeys = false;

    private void OnGUI() {

        if (!assigningKeys)
            return;

        Event e = Event.current;
        if (e.type == EventType.KeyDown && e.isKey && e.keyCode.ToString() != "None") {

            // Ignores Escape Key
            if (e.keyCode.ToString() == "Escape")
                return;

            // Ignores Repeated Key
            for (int i = 0; i < UserInputController.instance.playerKeyCode.Length; i++) {
                if (e.keyCode == UserInputController.instance.playerKeyCode[i]) {
                    StartCoroutine(ShowInfoText("This key has already been assigned, pick another!", 3f));
                    return;
                }

            }

            keyCodes[currentPlayer] = e.keyCode;
            currentPlayer++;

            if(currentPlayer >= 4){
                assigningKeys = false;
            }
            /*
            UserInputController.instance.playerKeyCode[currentPlayer - 1] = e.keyCode;
            currentPlayer++;
            if (currentPlayer >= 5) {
                lobbyPanel.DOFade(0f, 0.5f).OnComplete(() => {
                    LogicController.instance.StartMainGame();

                    // Set button spawner icons
                    ButtonSpawner.instance.icons = new Sprite[UserInputController.instance.playerKeyCode.Length];
                    for (int i = 0; i < UserInputController.instance.playerKeyCode.Length; i++) {
                        ButtonSpawner.instance.icons[i] = playerImages[i].sprite;
                    }

                    LogicController.instance.gameStart = true;

                    Destroy(gameObject);
                });
            }*/
        }
    }

    IEnumerator ShowInfoText(string content, float duration) {
        infoText.text = content;
        yield return new WaitForSeconds(duration);
        infoText.text = "";
    }


    private void Start() {
        DontDestroyOnLoad(this);
    }

    private void Update() {
        if(FindObjectOfType<UserInputController>()){
            FindObjectOfType<UserInputController>().playerKeyCode = keyCodes;
        }
    }
}