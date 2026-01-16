using UnityEngine;
using UnityEngine.UI;

[RequireComponent(typeof(Button))]
public class ButtonClickSFX : MonoBehaviour
{
    [Header("Sound Effect")]
    public AudioClip clickSFX;
    [Range(0f, 10f)] public float sfxVolume = 1.0f;

    private void Start()
    {
        Button button = GetComponent<Button>();
        button.onClick.AddListener(PlayClickSound);
    }

    private void PlayClickSound()
    {
        if (clickSFX != null)
        {
            // 一時的なAudioSourceを生成して2D音声で再生
            GameObject tempGO = new GameObject("TempButtonAudio");
            AudioSource aSource = tempGO.AddComponent<AudioSource>();

            aSource.clip = clickSFX;
            aSource.volume = sfxVolume;
            aSource.spatialBlend = 0f;  // 2D音声
            aSource.Play();

            // 再生後に自動破棄
            Destroy(tempGO, clickSFX.length);
        }
    }
}