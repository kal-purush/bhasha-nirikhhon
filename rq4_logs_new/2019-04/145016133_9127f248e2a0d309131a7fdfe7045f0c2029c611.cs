using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;
using UnityEngine.SceneManagement;
using TMPro;

using SeedQuest.Interactables;

public enum MenuScreenStates { Start, ModeSelect, SeedSetup, EncodeSeed, SceneLineUp, ActionLineUp }

public class MenuScreenManager : MonoBehaviour
{
    static private MenuScreenManager instance = null;
    static private MenuScreenManager setInstance() { instance = GameObject.FindObjectOfType<MenuScreenManager>(); return instance; }
    static public MenuScreenManager Instance { get { return instance == null ? setInstance() : instance; } }

    private MenuScreenStates state = MenuScreenStates.Start;
    private Canvas[] canvas;
    private Canvas startCanvas;
    private Canvas motionBackgroundCanvas;
    private Canvas encodeSeedCanvas;
    [HideInInspector]
    public Canvas encodeSeedContinueCanvas;
    private Canvas sceneLineUpCanvas;
    private Canvas actionLineUpCanvas;

    public void Awake() {
        canvas = GetComponentsInChildren<Canvas>(true);
        motionBackgroundCanvas = canvas[1];
        startCanvas = canvas[2];
        encodeSeedCanvas = canvas[5];
        encodeSeedContinueCanvas = canvas[6];
        sceneLineUpCanvas = canvas[7];
        actionLineUpCanvas = canvas[8];
    }

    public void Start() {
        motionBackgroundCanvas.gameObject.SetActive(true);
        GoToStart();
    }

    private void Update() {
        RotateBackground();
    }

    private void SetBackground(bool active) {
        motionBackgroundCanvas.gameObject.SetActive(active);
    }

    private void ResetCanvas() {
        canvas[2].gameObject.SetActive(false);
        canvas[3].gameObject.SetActive(false);
        canvas[4].gameObject.SetActive(false);
        canvas[5].gameObject.SetActive(false);
        canvas[6].gameObject.SetActive(false);
        canvas[7].gameObject.SetActive(false);
        canvas[8].gameObject.SetActive(false);
    }

    public void GoToStart() {
        state = MenuScreenStates.Start;
        ResetCanvas();
        startCanvas.gameObject.SetActive(true);
        SetupRotateBackground(0);
    }

    public void GoToModeSelect() {
        state = MenuScreenStates.ModeSelect;
        ResetCanvas();
        canvas[3].gameObject.SetActive(true);
        Debug.Log(state);
        SetupRotateBackground(150);
    }

    public void SetModeLearnSeed() { 
        GameManager.Mode = GameMode.Rehearsal;
        GoToSeedSetup();
    }

    public void SetModeRecoverSeed() { 
        GameManager.Mode = GameMode.Recall;
        GoToEncodeSeed();
    }

    public void GoToSeedSetup() {
        state = MenuScreenStates.SeedSetup;
        ResetCanvas();
        canvas[4].gameObject.SetActive(true);
        SetupRotateBackground(270);
        SetupSeedSetup();
    }

    public void GoToEncodeSeed() {
        state = MenuScreenStates.EncodeSeed;
        ResetCanvas();
        encodeSeedCanvas.gameObject.SetActive(true);
        SetupRotateBackground(330);
        SetupEncodeSeed();
    }

    public void GoToSceneLineUp() {
        state = MenuScreenStates.SceneLineUp;
        ResetCanvas();
        sceneLineUpCanvas.gameObject.SetActive(true);
        SetupSceneLineUp();
    }

    public void GoToActionLineUp() {
        state = MenuScreenStates.ActionLineUp;
        ResetCanvas();
        actionLineUpCanvas.gameObject.SetActive(true);
        SetupActionLineUp();
    }

    public void CloseActionLineUp() {
        actionLineUpCanvas.gameObject.SetActive(false);
        SetBackground(false);
    }

    public void SetupSeedSetup() {
        TMP_InputField seedInputField = GetComponentInChildren<TMP_InputField>();
        seedInputField.text = InteractablePathManager.SeedString;
        seedInputField.characterLimit = InteractableConfig.SeedHexLength; 
    }

    public void SetupEncodeSeed() {
        TMP_InputField seedInputField = GetComponentInChildren<TMP_InputField>(true);
        InteractablePathManager.SeedString = seedInputField.text;
    }

    public void SetupSceneLineUp() {
        LevelPanel[] panels = sceneLineUpCanvas.GetComponentsInChildren<LevelPanel>();
        int index = 0;
        foreach(LevelPanel panel in panels) {
            panel.GetComponentsInChildren<Image>()[1].sprite = LevelSetManager.CurrentLevels[index].preview;
            index++;
        }
        Image icon = sceneLineUpCanvas.GetComponentsInChildren<Image>()[1];
        icon.sprite = LevelSetManager.CurrentLevel.icon;
        TextMeshProUGUI text = sceneLineUpCanvas.GetComponentsInChildren<TextMeshProUGUI>()[2];
        text.text = LevelSetManager.CurrentLevel.name;

        StartScene();
    }

    public void SetupActionLineUp() {
        Image preview = actionLineUpCanvas.GetComponentsInChildren<Image>()[2];
        preview.sprite = LevelSetManager.CurrentLevel.preview;
        Image icon = actionLineUpCanvas.GetComponentsInChildren<Image>()[3];
        icon.sprite = LevelSetManager.CurrentLevel.icon;
        TextMeshProUGUI text = actionLineUpCanvas.GetComponentsInChildren<TextMeshProUGUI>()[0];
        text.text = LevelSetManager.CurrentLevel.name;               
    }

    AsyncOperation operation = null;
    IEnumerator LoadAsync(string sceneName)
    {
        yield return new WaitForSeconds(0.5f);

        operation = SceneManager.LoadSceneAsync(sceneName);
        operation.allowSceneActivation = false;

        while (!operation.isDone)
        {

            float progress = Mathf.Clamp01(operation.progress / 0.9f);
            //if (LoadingScreenUI.Instance != null)
            //    LoadingScreenUI.LoadProgress = progress;

            if (operation.progress >= 0.9f)
            {
                operation.allowSceneActivation = true;
            }

            yield return null;
        }
    }

    public void StartScene() {
        GameManager.State = GameState.Play;
        Instance.StartCoroutine(Instance.LoadAsync(LevelSetManager.CurrentLevel.scenename));
    }

    static public void SetLevelPanel(int panelIndex, int levelIndex) {
        
        LevelPanel selectedPanel = Instance.encodeSeedCanvas.GetComponentsInChildren<LevelPanel>()[panelIndex]; 
        TextMeshProUGUI text = selectedPanel.GetComponentsInChildren<TextMeshProUGUI>()[1];
        LevelSetManager.AddLevel(levelIndex);

        text.text = LevelSetManager.AllLevels[levelIndex].name;
        selectedPanel.GetComponentsInChildren<Image>()[2].sprite = LevelSetManager.AllLevels[levelIndex].preview;

        if(panelIndex == 3) {
            Instance.encodeSeedContinueCanvas.gameObject.SetActive(true);
        }
    }

    private Vector3 rotate = new Vector3(0, 0, 0);
    private Vector3 targetRotate = new Vector3(0, 0, 0);
    private float time = 0;
    public void SetupRotateBackground(float angle) {
        if(angle > 180)
            motionBackgroundCanvas.GetComponentsInChildren<Image>(true)[4].gameObject.SetActive(false);
        else
            motionBackgroundCanvas.GetComponentsInChildren<Image>(true)[4].gameObject.SetActive(true);

        rotate.z = targetRotate.z;
        targetRotate.z = angle;
        time = Time.time;

        Debug.Log(targetRotate);
    }

    public void RotateBackground() {
        float timeDuration = 1.0f;
        float t = Mathf.Clamp01((Time.time - time) / timeDuration);
        Vector3 newRotate = Vector3.Lerp(rotate, targetRotate, t);
        Debug.Log(newRotate);
        motionBackgroundCanvas.GetComponentInChildren<RectTransform>().localRotation = Quaternion.Euler(newRotate);
    }

    public void SetRandomSeed() {
        InteractablePathManager.SetRandomSeed();

        TMP_InputField seedInputField = GetComponentInChildren<TMP_InputField>();
        seedInputField.text = InteractablePathManager.SeedString;
    }
}