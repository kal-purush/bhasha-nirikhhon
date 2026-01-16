using UnityEngine;

public class DifficultySelector : MonoBehaviour
{
    public DifficultySetting difficultySetting;

    // Assign these methods to buttons in the UI
    public void SetEasyDifficulty()
    {
        difficultySetting.currentDifficulty = Difficulty.Easy;
    }

    public void SetNormalDifficulty()
    {
        difficultySetting.currentDifficulty = Difficulty.Normal;
    }

    public void SetHardDifficulty()
    {
        difficultySetting.currentDifficulty = Difficulty.Hard;
    }

    public void SetExtremeDifficulty()
    {
        difficultySetting.currentDifficulty = Difficulty.Extreme;
    }
}