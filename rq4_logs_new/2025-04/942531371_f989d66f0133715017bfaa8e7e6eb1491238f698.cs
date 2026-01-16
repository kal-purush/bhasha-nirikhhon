using UnityEngine;
using UnityEngine.Audio;

[CreateAssetMenu(fileName = "AudioPropsSO", menuName = "AudioPropsSO")]
public class AudioPropsSO: ScriptableObject
{
        [field: SerializeField] public AudioClip BGM { get; private set; }
        [field: SerializeField] public AudioClip ButtonClickSound { get; private set; }
        [field: SerializeField] public AudioClip ClickSound { get; private set; }
        [field: SerializeField] public AudioClip ProtectBreakSound { get; private set; }
        [field: SerializeField] public AudioClip RuneBreakSound { get; private set; }
        [field: SerializeField] public AudioClip RuneFallSound { get; private set; }
        [field: SerializeField] public AudioClip RuneMatchesSound { get; private set; }
        [field: SerializeField] public AudioClip StartGameSound { get; private set; }
        
        [field: SerializeField] public AudioMixerGroup MasterAudioMixerGroup { get; private set; }
        [field: SerializeField] public AudioMixerGroup BGMAudioMixerGroup { get; private set; }
        [field: SerializeField] public AudioMixerGroup SFXAudioMixerGroup { get; private set; }
}