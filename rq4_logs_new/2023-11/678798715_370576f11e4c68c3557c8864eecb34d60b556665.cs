using UnityEngine;

public class MainMenu : MonoBehaviour
{
    [SerializeField] private Game game;

    private int savedSize;
    
    public void PlayGame(int size)
    {
        savedSize = size;
        game.StartGame(size);
    }

    public void Restart()
    {
        GoToMenu();
        PlayGame(savedSize);
    }
    
    public void GoToMenu()
    {
        Destroy(game.InstCube.gameObject);
        Destroy(CubeRotation.Target.gameObject);
    }
    
    public void QuitGame()
    {
        Application.Quit();
    }
}