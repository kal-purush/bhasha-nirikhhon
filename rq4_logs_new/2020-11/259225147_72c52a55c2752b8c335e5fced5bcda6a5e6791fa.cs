using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class BgmManager : MonoBehaviour
{
    public enum BGM_NAME { a, b, c }

    [System.Serializable]
    public class MyBGM
    {
        public AudioClip startBgm;
        public AudioClip loopBgm;
        [Range(0f, 1f)] public float volume;
        public float bgmPlayTime;
        [HideInInspector] public bool isLoop;
    }

    public PrefabDictionary bgmData;

    [SerializeField] bool isBgmChange;
    [SerializeField] BGM_NAME playBgmName;
    [SerializeField] BGM_NAME nextBgmName;
    [SerializeField] float bgmChangeSpeed = 0.1f;

    int playAudio = 0;
    int standbyAudio = 1;
    BGM_NAME tmpNextBgmName;
    AudioSource[] audioSources;

    static readonly int kAudioSourceArrCount = 2;

    private void Reset()
    {
        if (GetComponent<AudioSource>() != null) return;
        for (int i = 0; i < kAudioSourceArrCount; i++)
        {
            var audio = gameObject.AddComponent<AudioSource>();
            audio.loop = false;
            audio.playOnAwake = false;
        }
    }

    // Start is called before the first frame update
    void Start()
    {
        audioSources = GetComponents<AudioSource>();

        //初期化
        SetBgmDataAll(playBgmName, audioSources[playAudio]);
        audioSources[standbyAudio].volume = 0;
        audioSources[playAudio].Play();
    }

    // Update is called once per frame
    void Update()
    {
        if(!isBgmChange) DebugBgmVolumeChange();

        BgmLoopChange(audioSources[playAudio]);

        //曲を変える
        if (isBgmChange)
        {
            //新しい曲をセット
            if (tmpNextBgmName != nextBgmName)
            {
                SetSecondAudio(nextBgmName);
                tmpNextBgmName = nextBgmName;
            }

            //フェード処理
            var isFade = BgmChangeFade(nextBgmName);
            if (!isFade)
            {
                SetBgmChangeEndProcess(nextBgmName);
                isBgmChange = false;
            }
        }
    }

    //AudioSourceにデータを入れる
    void SetBgmDataAll(BGM_NAME name, AudioSource audioSource)
    {
        var data = bgmData.GetTable()[name];
        audioSource.clip = data.isLoop ? data.loopBgm : data.startBgm;
        audioSource.loop = data.isLoop;
        audioSource.volume = data.volume;
        audioSource.time = data.bgmPlayTime;
    }

    //スタンバイ用のオーディオにBGMデータをセットする
    void SetSecondAudio(BGM_NAME name)
    {
        SetBgmDataAll(name, audioSources[standbyAudio]);
        audioSources[standbyAudio].volume = 0f;
    }

    //曲を入れ替えたときに呼ぶ
    void SetBgmChangeEndProcess(BGM_NAME nextName)
    {
        bgmData.GetTable()[playBgmName].bgmPlayTime = audioSources[playAudio].time;
        playBgmName = nextName;
        audioSources[playAudio].Stop();

        //入れ替え
        (playAudio, standbyAudio) = (standbyAudio, playAudio);
    }

    //フェード処理
    bool BgmChangeFade(BGM_NAME InBgmName)
    {
        var CchangeSpeed = bgmChangeSpeed * Time.deltaTime;
        var fadeOutBgm = audioSources[playAudio];
        var fadeInBgm = audioSources[standbyAudio];
        var inBgmData = bgmData.GetTable()[InBgmName];
        bool isFadePlay = true;

        //フェードイン用のBGMを流す
        if (!fadeInBgm.isPlaying)
        {
            fadeInBgm.Play();
        }

        //フェードアウト処理
        if (fadeOutBgm.volume != 0)
        {
            fadeOutBgm.volume -= CchangeSpeed;
        }
        else
        {
            isFadePlay = false;
        }

        //フェードイン処理
        if (fadeInBgm.volume < inBgmData.volume)
        {
            fadeInBgm.volume += CchangeSpeed;
        }
        else
        {
            return isFadePlay;
        }

        return true;
    }

    //ループ用のBgmに差し替える処理
    void BgmLoopChange(AudioSource audioSource)
    {
        var data = bgmData.GetTable()[playBgmName];
        if (data.isLoop) return;

        if (!audioSource.isPlaying)
        {
            audioSource.clip = data.loopBgm;
            audioSource.time = 0;
            audioSource.Play();
            audioSource.loop = true;
            data.isLoop = true;
        }
    }

    //ボリューム変更用
    void DebugBgmVolumeChange()
    {
        if (audioSources[playAudio].volume != bgmData.GetTable()[playBgmName].volume)
        {
            audioSources[playAudio].volume = bgmData.GetTable()[playBgmName].volume;
        }
    }

    /// <summary>
    /// フェードイン・アウトを利用してBGMを変える
    /// </summary>
    /// <param name="changeBgmName"></param>
    public void OnBgmChange(BGM_NAME changeBgmName)
    {
        nextBgmName = changeBgmName;
        isBgmChange = true;
    }

    /// <summary>
    /// フェードイン・アウト中はtrueを返す
    /// </summary>
    public bool GetIsBgmFadePlaying
    {
        get { return isBgmChange; }
    }

    [System.Serializable]
    public class PrefabDictionary : Serialize.TableBase<BGM_NAME, MyBGM, Name2Prefab> { }

    [System.Serializable]
    public class Name2Prefab : Serialize.KeyAndValue<BGM_NAME, MyBGM>
    {
        public Name2Prefab(BGM_NAME key, MyBGM value) : base(key, value) { }
    }
}