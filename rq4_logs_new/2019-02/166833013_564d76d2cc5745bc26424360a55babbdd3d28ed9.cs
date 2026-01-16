using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class N_AudioPlaying : MonoBehaviour
{

    // 중복 생성 방지
    private static N_AudioPlaying _instance;

    // Use this for initialization
    void Start()
    {
        if (!_instance)  // 사운드가 없으면 _instance는 이게 된다.
        {
            _instance = this;
        }

        else  // 만약 사운드가 이미 있으면 사운드를 없애 버려라.
        {
            Destroy(gameObject);
        }
        DontDestroyOnLoad(transform.gameObject);

        // 사운드 재생
        GetComponent<AudioSource>().Play();
    }

    // CardSystem 씬에 들어가면 제거
    private void OnLevelWasLoaded(int level)
    {
        if (level == 8)
            Destroy(gameObject);
    }

}