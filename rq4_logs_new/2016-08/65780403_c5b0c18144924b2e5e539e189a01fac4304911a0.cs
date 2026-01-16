using UnityEngine;
using UnityEngine.SceneManagement;
using UnityEngine.UI;

public class MenuControl : MonoBehaviour
{
    private static string previous;
        
    /*
     * Change scene to specific scene name 
     * Could also load a saved game
     */
    public void ChangeScene(string sceneName) {
        previous = SceneManager.GetActiveScene().name;
        SceneManager.LoadScene(sceneName);
    }

    /*
     * Changes scene to the previous scene 
     */
    public void Back() {
        ChangeScene(previous);
    }

    /*
     * Quits the game
     */
    public void Quit() {
        Application.Quit();
    }
}