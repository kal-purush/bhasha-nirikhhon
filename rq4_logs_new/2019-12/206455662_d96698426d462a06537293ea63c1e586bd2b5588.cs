using UnityEngine;
using UnityEngine.SceneManagement;

public class RulesGames : MonoBehaviour
{
    private string sceneReturnName = "Main_Menu";
    private string enterRulesScene = "Rules_Game";

    public void QuitRulesButton()
    {
        SceneManager.LoadScene(sceneReturnName);
    }

    public void EnterRulesScene()
    {
        SceneManager.LoadScene(enterRulesScene);
    }
}