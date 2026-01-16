using UnityEngine;

public class PlayerPrefsController : MonoBehaviour
{
    private const string MasterVolumeKey = "master volume";
    private const string DifficultyKey = "difficulty";

    private const float MinVolume = 0f;
    private const float MaxVolume = 1f;

    public static void SetMasterVolume(float volume)
    {
        if (volume >= MinVolume && volume <= MaxVolume)
        {
            PlayerPrefs.SetFloat(MasterVolumeKey, volume);
        }
        else
        {
            Debug.LogError("Master volume is out of range.");
        }
    }

    public static float GetMasterVolumeValue()
        => PlayerPrefs.GetFloat(MasterVolumeKey);

    public static void SetDifficulty(float difficulty)
    {
        PlayerPrefs.SetFloat(DifficultyKey, difficulty);
    }

    public static float GetDifficultyValue()
        => PlayerPrefs.GetFloat(DifficultyKey);
}