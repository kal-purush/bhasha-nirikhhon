using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ADX_CivControl : MonoBehaviour
{
    private CriAtomSource Sound;
    private GameObject player;
    public float distance = 20;
    float _distance;
    private float span = 0.1f;
    private float currentTime = 0f;
    private int CivilNum;
    private string CivilSelecter;
    private Rigidbody _rigidbody;
    CriAtomExPlayback CivilContact, CivilIdle, CivilFoot;
    // Start is called before the first frame update
    void Start()
    {
        player = GameObject.Find("Player");
        _rigidbody = this.GetComponent<Rigidbody>();
        Sound = GetComponent<CriAtomSource>();
    }

    // Update is called once per frame
    void Update()
    {
        //span秒に1回だけ処理する
        currentTime += Time.deltaTime;
        if (currentTime > span)
        {
            _distance = Vector3.Distance(player.transform.position, transform.position);
            if (_distance < distance)
            {
                PlayAndStopSound();
            }
            else
            {
                Sound.Stop();
            }
            //Debug.Log(_distance);
            //Debug.Log(Sound.status);
            currentTime = 0f;
        }
        if(_rigidbody.velocity.magnitude < 0.1f)
        {
            CivilFoot.Stop();
            CivilIdle.Stop();
            Sound.Play("Civil_Contact");
        }
    }

    //再生状況監視
    private void PlayAndStopSound()
    {
        if (Sound != null)
        {
            CriAtomSource.Status status = Sound.status;
            if ((status == CriAtomSource.Status.Stop) || (status == CriAtomSource.Status.PlayEnd))
            {
                //セレクターランダム値を決定
                CivilNum = Random.Range(7, 9);
                CivilSelecter = CivilNum.ToString();
                Sound.player.SetSelectorLabel("CivilVoice", CivilSelecter);

                CivilFoot = Sound.Play("Civil_Footstep00");
                CivilIdle =  Sound.Play("Civil_Idle");
            }
        }
    }
}