using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.EventSystems;
using UnityEngine.SceneManagement;
using UnityEngine.UI;

public class DetailsMenu : MonoBehaviour
{
    public MainMenuCameraControl camera;
    public Image detailsImage;
    public Text detailsComments;

    EventSystem events;
    LevelInfo level;
    void Start() {
        events = GetComponentInChildren<EventSystem>();
    }
    public void Init(LevelInfo level) {
        this.level = level;
        detailsImage.sprite = level.splashSprite;
        detailsComments.text = level.splashComments;
    }
    void Update() {
        if (!camera.lookAtLevelDetails) return;
        Focus();
    }
    void Focus() {
        if (events != null && events.currentSelectedGameObject == null) {
            Button bttn = events.firstSelectedGameObject.GetComponent<Button>();
            bttn.Select();
        }
    }
    public void ButtonClickPlay() {
        if (level == null) return;
        SceneManager.LoadScene(level.levelFilename, LoadSceneMode.Single);
    }
    public void ButtonClickBack() {
        camera.lookAtLevelDetails = false;
    }
}