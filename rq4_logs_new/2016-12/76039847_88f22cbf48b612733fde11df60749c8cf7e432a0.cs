using UnityEngine;

public class SoundManager : MonoBehaviour 
{
    //enum
    enum EffectType {
        BOOM, COIN, END
    }

    //singleton
    public static SoundManager instance = null;

	AudioSource BGMSource;
    AudioSource effectSource;
	AudioClip[] BGMList;
    AudioClip[] effectList = new AudioClip[(int)EffectType.END];

    void Awake() {
        instance = this;
        InitBGM();
        InitEffect();
    }

    void InitBGM() {
        BGMSource = gameObject.AddComponent<AudioSource>();
        BGMList = Resources.LoadAll<AudioClip>("BGM");
    }

    void InitEffect() {
        effectSource = gameObject.AddComponent<AudioSource>();
        effectList[(int)EffectType.BOOM] = Resources.Load<AudioClip>("EffectSound/Explosion");
        effectList[(int)EffectType.COIN] = Resources.Load<AudioClip>("EffectSound/Coin");
    }

    void Start() {
        BindFuncToEvent();
    }

    void BindFuncToEvent() {
        EventManager.instacne.AddFuncToEventForStart(NormalBGM, EventManager.EventType.NORMAL);
        EventManager.instacne.AddFuncToEventForStart(SpecialBGM, EventManager.EventType.SPECIAL);
        EventManager.instacne.AddFuncToEventForStart(SpecialBGM, EventManager.EventType.BRANCH);
        EventManager.instacne.AddFucToEventForDie(DieBGM);
    }

	void NormalBGM() {
        if (BGMSource.clip != BGMList[0]) {
            BGMSource.clip = BGMList [0];
            BGMSource.Play();
        }
	}

	void SpecialBGM() {
        if (BGMSource.clip != BGMList[1]) {
            BGMSource.clip = BGMList[1];
            BGMSource.Play();
        }
	}

	void DieBGM() {
		BGMSource.clip = BGMList [2];
		BGMSource.Play ();
	}

    public void PlayBoom() {
        effectSource.clip = effectList[(int)EffectType.BOOM];
        effectSource.Play();
    }

    public void PlayCoin() {
        effectSource.clip = effectList[(int)EffectType.COIN];
        effectSource.Play();
    }
}