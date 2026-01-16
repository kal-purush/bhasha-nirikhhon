using UnityEngine;
using UnityEngine.SceneManagement;

public class AcordHealth : MonoBehaviour
{
    public GameObject[] lifeIcon;
    public static int lifeCount = 5;

    private void loseHealth()
    {
        lifeCount--;
        if(lifeCount >= 0 && lifeCount <= lifeIcon.Length)
        {
            lifeIcon[lifeCount].SetActive(false);
        }

        if(lifeCount == 0)
        {
            SceneManager.LoadScene("MainMenu");
        }
    }
}