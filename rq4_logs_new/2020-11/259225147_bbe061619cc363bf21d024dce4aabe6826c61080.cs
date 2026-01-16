using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Beam : MonoBehaviour
{
    [SerializeField] GameObject beamObj;
    [SerializeField] Material material;
    [SerializeField] ParticleSystem beamParticle;
    [SerializeField] ParticleSystem startBlastParticle;

    public float beamRange = 5;
    public float lostStartTime = 5;
    public bool isGetEnd { get; private set; }
    public GameObject GetBeamObj => beamObj; 

    float timer;
    bool isMoveStop;
    Vector3 beamObjSiz;

    static readonly float kSizUpSpeed = 50f;
    static readonly float kDefaultSiz = 0f;
    static readonly float kDefaultAlpha = 1f;
    static readonly float kInvisibleAlpha = 0f;
    static readonly float kAlphaChangeSpeed = 0.5f;

    // Start is called before the first frame update
    void Start()
    {
        var beamCoreObj = beamObj.transform.GetChild(0);
        material = beamCoreObj.GetComponent<Renderer>().material = new Material(material);
        beamObjSiz = beamObj.transform.localScale;
        beamObjSiz.z = kDefaultSiz;
        beamObj.transform.localScale = beamObjSiz;
    }

    // Update is called once per frame
    void Update()
    {
        if (!isMoveStop)
        {
            if (timer < lostStartTime)
            {
                var time = Time.deltaTime;
                timer += time;

                //ビームのサイズ変更(伸ばす処理)
                if (beamObjSiz.z < beamRange)
                {
                    beamObjSiz = beamObj.transform.localScale;
                    beamObjSiz.z += kSizUpSpeed * time;
                    beamObj.transform.localScale = beamObjSiz;

                }
            }
            else
            {
                //時間が来たらParticleを止める
                beamParticle.Stop();
                startBlastParticle.Stop();
                isMoveStop = true;
                timer = kDefaultAlpha;
            }
        }
        else
        {
            if (timer > kInvisibleAlpha)
            {
                //ビームを透明にしていく
                timer -= kAlphaChangeSpeed * Time.deltaTime;
                timer = Mathf.Clamp(timer, kInvisibleAlpha, kDefaultAlpha);
                material.SetFloat("Vector1_73C72292", timer);
            }
            else
            {
                //演出が終わった時にフラグが立つ
                isGetEnd = true;
            }
        }
    }
}