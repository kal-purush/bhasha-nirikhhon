using UnityEngine;
using TMPro;

public class GameVersionDisplay : MonoBehaviour
{
    [SerializeField] TMP_Text versionText;

    private void Start()
    {
        if (versionText != null)
        {
            versionText.text = $"Version: {Application.version}";
        }
        else
        {
            Debug.LogWarning("GameVersionDisplay: TMP_Text �� ��������!");
        }
    }
}