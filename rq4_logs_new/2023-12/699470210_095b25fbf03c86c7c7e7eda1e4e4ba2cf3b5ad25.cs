using UnityEngine;
using UnityEngine.UI;

public class LevelUI : MonoBehaviour
{
    public Button level1BT;
    public Button level2BT;
    public Button level3BT;
    public Button level4BT;
    public Button level5BT;
    public Button level6BT;
    public Button level7BT;
    public Button level8BT;
    public Button level9BT;
    public Button level10BT;
    public Button level11BT;
    public Button level12BT;

    private void Start()
    {

        if (!LevelTracker.firstGate)
        {
            level2BT.gameObject.SetActive(false);
            level3BT.gameObject.SetActive(false);
            level4BT.gameObject.SetActive(false);
            level5BT.gameObject.SetActive(false);
            level6BT.gameObject.SetActive(false);
            level7BT.gameObject.SetActive(false);
            level8BT.gameObject.SetActive(false);
            level9BT.gameObject.SetActive(false);
            level10BT.gameObject.SetActive(false);
            level11BT.gameObject.SetActive(false);
            level12BT.gameObject.SetActive(false);
        }
        if (LevelTracker.firstGate)
        {
            level5BT.gameObject.SetActive(false);
            level6BT.gameObject.SetActive(false);
            level7BT.gameObject.SetActive(false);
            level8BT.gameObject.SetActive(false);
            level9BT.gameObject.SetActive(false);
            level10BT.gameObject.SetActive(false);
            level11BT.gameObject.SetActive(false);
            level12BT.gameObject.SetActive(false);
        }
        if (LevelTracker.secondGate)
        {
            level5BT.gameObject.SetActive(true);
            level6BT.gameObject.SetActive(false);
            level7BT.gameObject.SetActive(false);
            level8BT.gameObject.SetActive(false);
            level9BT.gameObject.SetActive(false);
            level10BT.gameObject.SetActive(false);
            level11BT.gameObject.SetActive(false);
            level12BT.gameObject.SetActive(false);
        }
        if (LevelTracker.thirdGate)
        {
            level6BT.gameObject.SetActive(true);
            level7BT.gameObject.SetActive(true);
            level8BT.gameObject.SetActive(true);
            level9BT.gameObject.SetActive(false);
            level10BT.gameObject.SetActive(false);
            level11BT.gameObject.SetActive(false);
            level12BT.gameObject.SetActive(false);
        }
        if (LevelTracker.fourthGate)
        {
            level9BT.gameObject.SetActive(true);
            level10BT.gameObject.SetActive(false);
            level11BT.gameObject.SetActive(false);
            level12BT.gameObject.SetActive(false);
        }
        if (LevelTracker.fifthGate)
        {
            level10BT.gameObject.SetActive(true);
            level11BT.gameObject.SetActive(true);
            level12BT.gameObject.SetActive(true); 
        }

    }
}