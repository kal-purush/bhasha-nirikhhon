using UnityEngine;
using UnityEngine.UI;

public class GameOverMenuGUI : SingletonMonobehaviour<GameOverMenuGUI>
{
    [SerializeField] private GameObject content = default;
    public GameObject Content => content;

    [Header("Menu Content")]
    [SerializeField] private Button go_ReturnHubButton = default;
    [SerializeField] private Button go_MainMenuButton = default;

    //======================================================================
    private void OnEnable()
    {
        go_ReturnHubButton.onClick.AddListener(() => SceneControlManager.Instance.RespawnPlayerAtHub());
        go_MainMenuButton.onClick.AddListener(() => SceneControlManager.Instance.LoadMainMenuWrapper());
    }

    //======================================================================
    public void SetContentActive(bool active)
    {
        content.SetActive(active);

        if (active)
            SceneControlManager.Instance.CurrentGameplayState = GameplayState.Pause;
    }
}