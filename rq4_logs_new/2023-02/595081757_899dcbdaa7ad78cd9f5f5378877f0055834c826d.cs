using UnityEngine;
using UnityEngine.Video;

public class CutsceneController : MonoBehaviour
{
    public static CutsceneController Instance { get; private set; }

    [SerializeField] private VideoPlayer introCutscenePlayer;
    
    private void Awake()
    {
        if (Instance != null && Instance != this)
        {
            Destroy(this);
        }
        else
        {
            Instance = this;
        }
    }

    public void PlayIntroCutscene()
    {
        introCutscenePlayer.Play();
    }
}