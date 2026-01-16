using UnityEngine;
using System.Collections;
using System.Collections.Generic;

public class AudioManager : MonoBehaviour
{

    Dictionary<int, AudioSource> _bgm_list = new Dictionary<int, AudioSource>();
    Dictionary<int, AudioSource> _se_list = new Dictionary<int, AudioSource>();

    List<AudioSource> player_attack_se_list = new List<AudioSource>();
    List<AudioSource> player_damage_se_list = new List<AudioSource>();
    List<AudioSource> class_student_arrow_se_list = new List<AudioSource>();
    List<AudioSource> koutyou_arrow_se_list = new List<AudioSource>();
    List<AudioSource> student_master_arrow_se_list = new List<AudioSource>();

    [SerializeField]
    AudioClip[] BGM_LIST = null;

    [SerializeField]
    AudioClip[] SE_LIST = null;

    [SerializeField]
    AudioClip[] PLAYER_ATTACK_SE = null;

    [SerializeField]
    AudioClip[] PLAYER_DAMAGE_SE = null;

    [SerializeField]
    AudioClip[] CLASS_STUDENT_ARROW_SE = null;

    [SerializeField]
    AudioClip[] KOUTYOU_ARROW_SE = null;

    [SerializeField]
    AudioClip[] STUDENT_MASTER_ARROW_SE = null;


    public void PlayBGM(int id)
    {
        if (!_bgm_list.ContainsKey(id)) return;
        if (_bgm_list[id].isPlaying) return;
        _bgm_list[id].Play();
        _bgm_list[id].loop = true;
    }

    public void StopBGM(int id)
    {
        if (!_bgm_list.ContainsKey(id)) return;
        _bgm_list[id].Stop();
    }

    public void StopAllBGM()
    {
        foreach (var audio_source in _bgm_list)
        {
            audio_source.Value.Stop();
        }
    }

    public void PlaySE(int id)
    {
        if (!_se_list.ContainsKey(id)) return;
        if (_se_list[id].isPlaying) return;
        _se_list[id].Play();
    }

    public void StopSE(int id)
    {
        if (!_se_list.ContainsKey(id)) return;
        _se_list[id].Stop();
    }

    public void StopAllSE()
    {
        foreach (var audio_source in _se_list)
        {
            audio_source.Value.Stop();
        }
    }

    public void RandomPlayPlayerAttackSE()
    {
        int num = Random.Range(0, PLAYER_ATTACK_SE.Length);
        player_attack_se_list[num].Play();
    }

    public void RandomPlayPlayerDamageSE()
    {
        int num = Random.Range(0, PLAYER_DAMAGE_SE.Length);
        player_damage_se_list[num].Play();
    }

    public void RandomPlayClassStudentArrowSE()
    {
        int num = Random.Range(0, CLASS_STUDENT_ARROW_SE.Length);
        class_student_arrow_se_list[num].Play();
    }

    public void RandomPlayKoutyouArrowSE()
    {
        int num = Random.Range(0, KOUTYOU_ARROW_SE.Length);
        koutyou_arrow_se_list[num].Play();
    }

    public void RandomPlayStudentMasterArrowSE()
    {
        int num = Random.Range(0, STUDENT_MASTER_ARROW_SE.Length);
        student_master_arrow_se_list[num].Play();
    }



    void Awake()
    {
        for (var i = 0; i < BGM_LIST.Length; ++i)
        {
            _bgm_list.Add(i, new AudioSource());
            _bgm_list[i].clip = BGM_LIST[i];
        }

        for (var i = 0; i < SE_LIST.Length; ++i)
        {
            _se_list.Add(i, new AudioSource());
            _se_list[i].clip = SE_LIST[i];
        }

        for (var i = 0; i < PLAYER_ATTACK_SE.Length; ++i)
        {
            player_attack_se_list.Add(new AudioSource());
            player_attack_se_list[i].clip = PLAYER_ATTACK_SE[i];
        }

        for (var i = 0; i < PLAYER_DAMAGE_SE.Length; ++i)
        {
            player_damage_se_list.Add(new AudioSource());
            player_damage_se_list[i].clip = PLAYER_DAMAGE_SE[i];
        }

        for (var i = 0; i < CLASS_STUDENT_ARROW_SE.Length; ++i)
        {
            class_student_arrow_se_list.Add(new AudioSource());
            class_student_arrow_se_list[i].clip = CLASS_STUDENT_ARROW_SE[i];
        }

        for (var i = 0; i < KOUTYOU_ARROW_SE.Length; ++i)
        {
            koutyou_arrow_se_list.Add(new AudioSource());
            koutyou_arrow_se_list[i].clip = KOUTYOU_ARROW_SE[i];
        }

        for (var i = 0; i < STUDENT_MASTER_ARROW_SE.Length; ++i)
        {
            student_master_arrow_se_list.Add(new AudioSource());
            student_master_arrow_se_list[i].clip = STUDENT_MASTER_ARROW_SE[i];
        }
    }
}