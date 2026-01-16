using UnityEngine;

public class LevelChooser : MonoBehaviour
{
    // Start is called before the first frame update
    void Awake()
    {
        switch(PlayerPrefs.GetString("level"))
        {
            case "LevelTuto":
                GetComponent<LevelTuto>().enabled = true;
                break;
            case "LevelEndless":
                GetComponent<LevelEndless>().enabled = true;
                break;
            case "Level1":
                GetComponent<Level1>().enabled = true;
                break;
            case "Level2":
                GetComponent<Level2>().enabled = true;
                break;
            case "Level3":
                GetComponent<Level3>().enabled = true;
                break;
        }
    }
}