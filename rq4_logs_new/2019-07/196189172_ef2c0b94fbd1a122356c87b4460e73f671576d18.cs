using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;
using UnityEngine.Video;

using NatCorderU.Core;


public class VideoFactoryAgent : MonoBehaviour
{
    // 视频幕布
    [SerializeField, Header("UI")] RawImage _image;
    [SerializeField] VideoPlayer _videoPlayer;
    [SerializeField] Camera _camera;
    [SerializeField] Animator _animator;

    [SerializeField] Image[] content1s = new Image[2];
    [SerializeField] Image[] content2s = new Image[2];
    [SerializeField] Image[] content3s = new Image[2];


    [SerializeField] bool _record;



    private bool _videoIsPrepared = false;  // 视频加载情况标识符

    private List<string> _photos = new List<string>();   // 照片集合

    private VideoFactoryStatus _videoFactoryStatus = VideoFactoryStatus.Prepare;


    enum VideoFactoryStatus {
        Prepare,
        Active,
        Running,
        Finish
    }

    private bool doRunLock = false;
    private bool doActiveLock = false;
    private bool doFinishLock = false;


    private void Reset() {
        doRunLock = false;
        doActiveLock = false;
        doFinishLock = false;

        _videoFactoryStatus = VideoFactoryStatus.Prepare;
    }


    /// <summary>
    ///     激活视频工厂
    /// </summary>
    public void DoActive() {
        _videoFactoryStatus = VideoFactoryStatus.Active;
    }



    // Start is called before the first frame update
    void Start()
    {
        Reset();

        if (!_videoIsPrepared)
        {
            //LoadVideo();
        }
        else
        {
            //CompletePrepare();
        }
    }

    // Update is called once per frame
    void Update()
    {
        if (Input.GetKeyDown(KeyCode.M)) {
            StopRecord();
        }


        if (Input.GetKeyDown(KeyCode.Space))
        {
            // 开始动画
            _animator.Play("Play");
        }


        if (_videoFactoryStatus == VideoFactoryStatus.Active) {

            if (!doActiveLock) {
                doActiveLock = true;
                Sprite sprite = null, sprite2 = null, sprite3 = null;

                // 获取三张照片 / 处理照片 
                for (int i = 0; i < _photos.Count; i++)
                {
                    string photoStr = _photos[i];
                }

                // 将图片赋值至控件
                for (int i = 0; i < content1s.Length; i++)
                {
                    content1s[i].sprite = sprite;
                }

                for (int i = 0; i < content2s.Length; i++)
                {
                    content2s[i].sprite = sprite2;
                }

                for (int i = 0; i < content3s.Length; i++)
                {
                    content3s[i].sprite = sprite3;
                }


                _videoFactoryStatus = VideoFactoryStatus.Running;
            }
           
        }

        if (_videoFactoryStatus == VideoFactoryStatus.Running) {
            if (!doRunLock)
            {
                doRunLock = true;

                if (_record)
                {
                    //  开始录屏
                    StartRecord();
                }

                //  开始动画
                _animator.Play("Play");

            }
        }

        if (_videoFactoryStatus == VideoFactoryStatus.Finish)
        {
            if (!doFinishLock)
            {
                doFinishLock = true;

                if (_record)
                {
                    //  结束录屏
                    StopRecord();

                    //  生成视频文件

                }

                //  结束并重置
                Reset();

            }
        }

        // 判断动画状态
        AnimatorStateInfo stateInfo = _animator.GetCurrentAnimatorStateInfo(0);




    }


    private void LoadVideo()
    {
        StartCoroutine(PrepareVideo());
    }

    //step1
    //播放视频
    IEnumerator PrepareVideo()
    {
        _videoPlayer.Prepare();
        while (!_videoPlayer.isPrepared)
        {
            yield return new WaitForSeconds(1);
            break;
        }
        _videoIsPrepared = true;

        _image.texture = _videoPlayer.texture;

        _videoPlayer.Play();

        StartRecord();

    }

    /// <summary>
    ///     开始录屏
    /// </summary>
    private void StartRecord() {


        // Create a recording configuration
        const float DownscaleFactor = 2f / 3;

        int w = (int)(Screen.width * DownscaleFactor);
        int h = (int)(Screen.height * DownscaleFactor);

        Debug.Log("W : " + w + " | H : " + h);


        var configuration = new Configuration(w, h);
        // Start recording with microphone audio
        //if (recordMicrophoneAudio)
        //{
        //    // Start the microphone
        //    audioSource.clip = Extensions.Microphone.StartRecording();
        //    audioSource.loop = true;
        //    audioSource.Play();
        //    // Start recording with microphone audio
        //    Replay.StartRecording(Camera.main, configuration, OnReplay, audioSource, true);
        //}
        // Start recording without microphone audio
        Replay.StartRecording(_camera, configuration, OnReplay);


    }

    /// <summary>
    ///     停止录屏
    /// </summary>
    private void StopRecord() {

        // Stop playing mic audio
        //if (recordMicrophoneAudio)
        //{
        //    audioSource.Stop();
        //    Extensions.Microphone.StopRecording();
        //}
        // Stop recording
        Replay.StopRecording();

    }


    void OnReplay(string path)
    {
        Debug.Log("Saved recording to: " + path);
#if UNITY_IOS || UNITY_ANDROID
            // Playback the video
            Handheld.PlayFullScreenMovie(path);
#endif
    }

}