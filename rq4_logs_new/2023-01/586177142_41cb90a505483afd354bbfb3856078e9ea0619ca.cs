using UnityEngine;
using UnityEngine.SceneManagement;

public class RestartManager : MonoBehaviour
{
    public void Restart() {
        SceneManager.LoadScene(1);
    }
}