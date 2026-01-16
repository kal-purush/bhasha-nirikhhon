using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ADX_CivControl : MonoBehaviour
{
    private CriAtomSource cas;
    private GameObject player;
    private float distance = 10;
    private float span = 0.1f;
    private float currentTime = 0f;
    private Rigidbody rb;
    CriAtomExPlayback CivilContact, CivilIdle, CivilFoot;
    public bool NoWalking = false;

    private bool isStop;
    // Start is called before the first frame update
    void Start()
    {
        player = GameObject.Find("Player");
        rb = this.GetComponent<Rigidbody>();
        cas = GetComponent<CriAtomSource>();
    }

    // Update is called once per frame
    void Update()
    {
        //span秒に1回だけ処理する
        currentTime += Time.deltaTime;

        if (currentTime > span)
        {
            currentTime = 0f;
            if (Vector3.Distance(player.transform.position, transform.position) < distance)
            {
                PlayAndStopSound();
            }
            else
            {
                cas.Stop();
            }
            //Debug.Log(_distance);
            //Debug.Log(Sound.status);

            if (rb.velocity.magnitude < 0.1f && NoWalking == false)
            {
                Stoping();
                isStop = true;
            }
            else isStop = false;
        }
    }

    //再生状況監視
    private void PlayAndStopSound()
    {
        if ((cas.status == CriAtomSource.Status.Stop) || (cas.status == CriAtomSource.Status.PlayEnd))
        {
            //セレクターランダム値を決定
            cas.player.SetSelectorLabel("CivilVoice", Random.Range(1, 9).ToString());

            if (!isStop && NoWalking == false) CivilFoot = cas.Play("Civil_Footstep00");
            CivilIdle = cas.Play("Civil_Idle");
        }
    }

    private void Stoping()
    {
        if (isStop) return;
        CivilFoot.Stop();
        CivilIdle.Stop();
        cas.Play("Civil_Contact");
    }

    private void OnCollisionEnter(Collision other)
    {
        if (other.gameObject.tag == "Player" && NoWalking == true) {
            CivilIdle.Stop();
            cas.Play("Civil_Contact");
        }
    }
}