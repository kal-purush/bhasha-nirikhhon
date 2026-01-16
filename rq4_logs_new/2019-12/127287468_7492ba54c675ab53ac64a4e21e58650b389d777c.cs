using UnityEngine;

/// <summary>
/// A manager that handles music and sound effects
/// </summary>
public class SoundManager : MonoBehaviour {
    public static SoundManager Instance;

    [Header("Sound Tracks")]
    public AudioClip[] Bgm;
    public AudioClip[] Sfx;

    [Header("Audio Sources")]
    public AudioSource BGMAudio;
    public AudioSource SFXAudio;

    private float bgmVolume = 0.5f;
    private float sfxVolume = 0.5f;

    // Start is called before the first frame update
    void Start() {
        if (Instance == null) {
            Instance = this;
            BGMAudio.volume = bgmVolume;
            SFXAudio.volume = sfxVolume;
        } else {
            Destroy(gameObject);
        }
        DontDestroyOnLoad(gameObject);
    }

    /// <summary>
    /// Play a BGM track
    /// </summary>
    /// <param name="bgm">Track that is to be played</param>
    public void PlayBGM(int bgm) {
        if (bgm < Bgm.Length && (BGMAudio.clip == null || !BGMAudio.clip.Equals(Bgm[bgm])) ) {
            // make sure that the track is an index in the bgm array
            // also do not change the song if it is the same one being played
            if (BGMAudio.isPlaying) {
                BGMAudio.Stop();
            }
            BGMAudio.clip = Bgm[bgm];
            BGMAudio.Play();
        }
    }

    /// <summary>
    /// Play a sound effect
    /// </summary>
    /// <param name="sfx">Sound effect that is to be played</param>
    public void PlaySFX(int sfx) {
        if (sfx < Sfx.Length) {
            // make sure that the track is an index in the sfx array
            SFXAudio.clip = Sfx[sfx];
            SFXAudio.Play();
        }
    }
}