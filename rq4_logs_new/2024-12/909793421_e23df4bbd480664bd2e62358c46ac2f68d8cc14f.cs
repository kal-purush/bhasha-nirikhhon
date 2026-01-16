using UnityEngine;
using UnityEngine.UI;

public class ButtonAssigner : MonoBehaviour
{
    public Button[] buttons; // Array to hold references to your buttons
    public SceneChanger sceneChanger; // Reference to your SceneChanger script

    private void Start()
    {
        for (int i = 0; i < buttons.Length; i++)
        {
            int sceneNumber = i; // Local copy of the loop variable
            buttons[i].onClick.AddListener(() => sceneChanger.SceneChange(sceneNumber));
        }
    }
}