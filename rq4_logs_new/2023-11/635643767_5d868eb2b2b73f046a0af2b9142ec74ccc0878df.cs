using UnityEngine;

public class ShowGameoverMenu : MonoBehaviour
{
    [SerializeField] private PlayerHeart playerHeartManager;
    [SerializeField] private GameOverMenuGUI gameOverMenu;

    //===========================================================================
    private void OnEnable()
    {
        playerHeartManager.OnDespawnPlayerEvent += DisplayGameOverUI_OnDespawnEventHandler;
    }

    private void OnDisable()
    {
        playerHeartManager.OnDespawnPlayerEvent -= DisplayGameOverUI_OnDespawnEventHandler;
    }

    //===========================================================================
    private void DisplayGameOverUI_OnDespawnEventHandler(object sender, System.EventArgs e)
    {
        if (gameOverMenu.Content.activeSelf)
            return;

        gameOverMenu.SetContentActive(true);
    }
}