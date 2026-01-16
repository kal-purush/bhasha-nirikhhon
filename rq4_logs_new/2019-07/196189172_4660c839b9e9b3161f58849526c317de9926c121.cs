using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;
using UnityEngine.Video;

using NatCorderU.Core;

public class VideoFactoryNoLogoAgent : MonoBehaviour
{
    // 视频幕布
    [SerializeField, Header("Video 1")] RawImage _image1;
    [SerializeField] VideoPlayer _videoPlayer1;

    [SerializeField, Header("Video 2")] RawImage _image2;
    [SerializeField] VideoPlayer _videoPlayer2;

    [SerializeField, Header("Video 3")] VideoPlayer _videoPlayer3;
    [SerializeField] RawImage _image3;

    [SerializeField, Header("Camera")] Camera _camera;
    [SerializeField] Animator _animator;


    [SerializeField, Header("Photo")] Image _scenario2left;
    [SerializeField] Image _scenario2middle;
    [SerializeField] Image _scenario2right;
    [SerializeField] Image _scenario3B;
    [SerializeField] Image _scenario3A;
    [SerializeField] Image _scenario3C;
    [SerializeField] Image _scenario4B;
    [SerializeField] Image _scenario4C;

    [SerializeField, Header("Message")] MessageBoxAgent _agent;


    [SerializeField] bool _record;
    [SerializeField, Header("Mock")] bool _isMock;

    //[SerializeField] ReplayCam _replayCam;

    private float _delayTime = 1f;

    private bool _isLoop = false; //  是否循环
    private bool _isRecorded = false; // 是否已录制

    private bool _video1Prepared = false;
    private bool _video2Prepared = false;
    private bool _video3Prepared = false;
    private bool _video4Prepared = false;

    private bool _sourceVideoIsPrepared = false;  // 视频源标识符

    private bool _videoIsPrepared = false;  // 视频加载情况标识符

    private List<string> _photos = new List<string>();   // 照片集合
    private List<Texture2D> _photoTexs = new List<Texture2D>();

    private VideoFactoryStatus _videoFactoryStatus = VideoFactoryStatus.Disabled;
    private VideoFactoryContentStatus _contentStatus = VideoFactoryContentStatus.Prepared;

    private Action<string> _videoGenerateCompletedCallback;
    private string _videoAddress;


    enum VideoFactoryStatus {
        Disabled,
        Prepare,
        Active,
        Running,
        Finish
    }

    enum VideoFactoryContentStatus {
        Prepared,
        Finish
    }

    private bool doPreparedLock = false;
    private bool doRunLock = false;
    private bool doActiveLock = false;
    private bool doFinishLock = false;



    private void Reset()
    {
        ResetLock();
        //_videoAddress = Application.persistentDataPath;

        _photoTexs = new List<Texture2D>();
        _videoFactoryStatus = VideoFactoryStatus.Disabled;
        _contentStatus = VideoFactoryContentStatus.Prepared;
    }

    private void ResetLock() {
        doPreparedLock = false;
        doRunLock = false;
        doActiveLock = false;
        doFinishLock = false;
    }

    private void ResetForInit()
    {
        _isLoop = false; //  是否循环
        _isRecorded = false; // 是否已录制
        _video1Prepared = false;
        _video2Prepared = false;
        _video3Prepared = false;
        _sourceVideoIsPrepared = false;  // 视频源标识符
        _videoIsPrepared = false;  // 视频加载情况标识符

    }



    #region Event In Animation

    public void PlaySecnarioOne() {
        //_videoPlayer1.frame = 0;
        _videoPlayer1.Play();
        Debug.Log("Play Secnario One");
    }

    public void StopSecnarioOne()
    {
        _videoPlayer1.Pause();

        Debug.Log("Stop Secnario One");
    }

    public void PlaySecnarioTwo()
    {
        _image2.texture = _videoPlayer2.texture;

        _videoPlayer2.Play();
        Debug.Log("Play Secnario Two");
    }

    public void StopSecnarioTwo()
    {
        _videoPlayer2.Pause();
        Debug.Log("Stop Secnario Two");
    }

    public void PlaySecnarioFive()
    {
        _image3.texture = _videoPlayer3.texture;

        _videoPlayer3.Play();
        Debug.Log("Play Secnario Five");
    }

    public void StopSecnarioFive()
    {
        _videoPlayer3.Pause();
        Debug.Log("Stop Secnario Five");
    }

    public void OnAnimationEnd() {


        Debug.Log("Stop OnAnimationEnd");

        _videoFactoryStatus = VideoFactoryStatus.Finish;
    }

    #endregion




    /// <summary>
    ///     激活视频工厂
    /// </summary>
    public void DoActive(Action<string> completeCallBack) {
        //Reset();

        ResetForInit();

        _videoGenerateCompletedCallback = completeCallBack;

        _videoFactoryStatus = VideoFactoryStatus.Prepare;
    }



    // Start is called before the first frame update
    //void Start()
    //{
    //    Reset();

    //    if (!_videoIsPrepared)
    //    {
    //        //LoadVideo();
    //    }
    //    else
    //    {
    //        //CompletePrepare();
    //    }
    //}

    // Update is called once per frame
    void Update()
    {
        //if (Input.GetKeyDown(KeyCode.M)) {
        //    StopRecord();
        //}


        if (Input.GetKeyDown(KeyCode.Space))
        {
            // 开始动画
            //_animator.Play("Play");
            DoActive(DoAfterCompleted);

        }

        if (_videoFactoryStatus == VideoFactoryStatus.Prepare)
        {

            if (!doPreparedLock) {
                doPreparedLock = true;

                if (!gameObject.activeSelf)
                {
                    gameObject.SetActive(true);
                }

                Sprite spriteA, spriteB, spriteC;
                if (_isMock)
                {

                    spriteA = Resources.Load<Sprite>("1");

                    Debug.Log("spriteA width : " + spriteA.rect.width);

                     spriteB = Resources.Load<Sprite>("2");
                     spriteC = Resources.Load<Sprite>("3");
                }
                else {
                     spriteA = Sprite.Create(_photoTexs[0], new Rect(0, 0, _photoTexs[0].width, _photoTexs[0].height), Vector2.zero);
                     spriteB = Sprite.Create(_photoTexs[1], new Rect(0, 0, _photoTexs[1].width, _photoTexs[1].height), Vector2.zero);
                     spriteC = Sprite.Create(_photoTexs[2], new Rect(0, 0, _photoTexs[2].width, _photoTexs[2].height), Vector2.zero);
                }




                _scenario2left.sprite = spriteA;
                _scenario2middle.sprite = spriteA;
                _scenario2right.sprite = spriteA;

                _scenario3A.sprite = spriteA;
                _scenario3B.sprite = spriteB;
                _scenario3C.sprite = spriteC;

                _scenario4B.sprite = spriteB;
                _scenario4C.sprite = spriteC;


                //  准备好视频
                LoadVideo();
            }

            if (!_sourceVideoIsPrepared)
            {
                CheckVideoStatus();
            }

            if (_sourceVideoIsPrepared)
            {
                Debug.Log("_sourceVideoIsPrepared");

                _videoFactoryStatus = VideoFactoryStatus.Running;
            }
        }



        if (_videoFactoryStatus == VideoFactoryStatus.Active) {

            Debug.Log("3");


            if (!doActiveLock) {
                doActiveLock = true;

                Debug.Log("2");


                CheckVideoStatus();

                Debug.Log("1111");


                _videoFactoryStatus = VideoFactoryStatus.Running;
            }

        }



        if (_videoFactoryStatus == VideoFactoryStatus.Running) {
            if (!doRunLock)
            {
                doRunLock = true;
                Debug.Log("Run!");


                if (_record && (!_isRecorded))
                {
                    Debug.Log("开始录屏");

                    //  开始录屏
                    StartRecord();
                }

                //  开始动画
                //_animator.Update(0f);
                _animator.Play("Play");

            }
        }

        if (_videoFactoryStatus == VideoFactoryStatus.Finish)
        {
            if (!doFinishLock)
            {
                doFinishLock = true;

                if (_record && !_isRecorded)
                {
                    //  结束录屏
                    Debug.Log("结束录屏");

                    StopRecord();

                    _isRecorded = true;

                    _videoGenerateCompletedCallback.Invoke(_videoAddress);
                }


                //  结束并重置
                Reset();
                
            }
        }


    }


    /// <summary>
    ///     添加 Photo Texture
    /// </summary>
    public void AddPhotoTexture(Texture2D photoTex) {
        if (_contentStatus == VideoFactoryContentStatus.Finish)
            return;

        _photoTexs.Add(photoTex);

        if (_photoTexs.Count == 3) {
            _contentStatus = VideoFactoryContentStatus.Finish;
        }
    }


    private void LoadVideo()
    {
        StartCoroutine(PrepareVideo1());
        StartCoroutine(PrepareVideo2());
        StartCoroutine(PrepareVideo3());
    }

    //step1
    //播放视频
    IEnumerator PrepareVideo1()
    {
        _videoPlayer1.Prepare();
        while (!_videoPlayer1.isPrepared)
        {
            Debug.Log("_videoPlayer1 is prepared");

            yield return new WaitForSeconds(1);
            break;
        }
        _video1Prepared = true;
        _image1.texture = _videoPlayer1.texture;

        //StartRecord();
    }

    IEnumerator PrepareVideo2()
    {
        _videoPlayer2.Prepare();
        while (!_videoPlayer2.isPrepared)
        {
            yield return new WaitForSeconds(1);
            break;
        }
        _video2Prepared = true;
        _image2.texture = _videoPlayer2.texture;

        //StartRecord();
    }

    IEnumerator PrepareVideo3()
    {
        _videoPlayer3.Prepare();
        while (!_videoPlayer3.isPrepared)
        {
            yield return new WaitForSeconds(1);
            break;
        }
        _video3Prepared = true;
        _image3.texture = _videoPlayer3.texture;

        //StartRecord();
    }


    private void CheckVideoStatus(){
        if (_video1Prepared && _video2Prepared && _video3Prepared)
        {
            //if (_videoPlayer4 == null)
            //{
            //    _sourceVideoIsPrepared = true;

            //}
            //else {
            //    if (_video4Prepared)
            //    {
            //        _sourceVideoIsPrepared = true;
            //    }
            //    else {
            //        _sourceVideoIsPrepared = false;

            //    }

            //}
             _sourceVideoIsPrepared = true;


        }
        else {
            _sourceVideoIsPrepared = false;
        }

        //Debug.Log("_sourceVideoIsPrepared : " + _sourceVideoIsPrepared);

    }



    /// <summary>
    ///     开始录屏
    /// </summary>
    private void StartRecord() {

        StartCoroutine(StartRecodingE());

    }


    IEnumerator StartRecodingE() {
        yield return new WaitForSeconds(0.2f);

        //_replayCam.StartRecording();


        // Create a recording configuration
        const float DownscaleFactor = 3 / 3;

        int w = (int)(Screen.width * DownscaleFactor);
        int h = (int)(Screen.height * DownscaleFactor);

        var configuration = new Configuration(w, h,60);
        //Replay.StartRecording(_camera, configuration, OnReplay);


        Replay.StartRecording(_camera, configuration, OnReplay);

    }


    /// <summary>
    ///     停止录屏
    /// </summary>
    private void StopRecord() {

        Replay.StopRecording();
        //_replayCam.StopRecording();


    }

    public void DoAfterCompleted(string str) {
        Debug.Log("Do After Completed");
    }


    void OnReplay(string path)
    {
        //_agent.UpdateMessageTemp(path);
        Debug.Log("Saved recording to: " + path);

        _videoAddress = path;

        _videoGenerateCompletedCallback.Invoke(path);

        Reset();
    }

    /// <summary>
    /// 获取视频地址
    /// </summary>
    /// <returns></returns>
    public string GetVideoAddress() {
        return _videoAddress;
    }



    /// <summary>
    ///     清理
    /// </summary>
    public void Clear() {
        Reset();
    }


    /// <summary>
    /// 停止播放
    /// </summary>
    public void StopPlayVideoAndClear() {

        // 如果正在录制中
        if (!_isRecorded) {
            _animator.StopRecording();
            Replay.StopRecording();
        }

        Reset();
    }

    public void StopPlayVideo()
    {

        // 如果正在录制中
        if (!_isRecorded)
        {
            _animator.StopRecording();
            Replay.StopRecording();
        }

        _isLoop = false;

    }
}