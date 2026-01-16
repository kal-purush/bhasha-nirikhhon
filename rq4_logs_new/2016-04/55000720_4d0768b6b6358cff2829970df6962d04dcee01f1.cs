using UnityEngine;
using UnityEngine.SceneManagement;

public class GameManager : MonoBehaviour
{
    private static GameManager _instance = null;

    bool _planGame = true;
    bool _planAvailable = false;

    public enum Levels { MENU, Scene2D_1, Scene3D_1 };

    public static GameManager Instance
    {
        get
        {
            if ( _instance == null )
            {
                _instance = FindObjectOfType<GameManager>();
            }
            return _instance;
        }
    }

    void Start ()
    {
        DontDestroyOnLoad( gameObject );
	}
	

    public void PlayGame()
    {
        _planGame = false;
        SceneManager.LoadScene((int)Levels.Scene3D_1);
        Debug.Log("Play the game.");
    }

    public void PlanGame()
    {
        _planGame = true;
        SceneManager.LoadScene((int)Levels.Scene2D_1);
        Debug.Log("Plan the game.");
    }

    public void ExitGame()
    {
        Debug.Log("Exit the game.");

        Application.Quit();
    }
}