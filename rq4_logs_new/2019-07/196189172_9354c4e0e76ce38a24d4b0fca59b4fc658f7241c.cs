using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.Video;

public class VideoPlayResAgent : MonoBehaviour
{
    // Start is called before the first frame update
    [SerializeField] VideoPlayer _videoPlayerRecordFirst;
    [SerializeField] VideoPlayer _videoPlayerRecordNoLogoFirst;
    [SerializeField] VideoPlayer _videoPlayerSecond;
    [SerializeField] VideoPlayer _videoPlayerThird;

    [SerializeField] VideoPlayer _videoPlayerDemo;
    [SerializeField] VideoPlayer _videoPlayerDemoNoLogo;

    [SerializeField] MessageBoxAgent _messageBoxAgent;



    public enum VideoPlayerType {
        videoPlayerDemo, videoPlayerDemoNoLogo, videoPlayerRecordFirst, videoPlayerRecordNoLogoFirst, videoPlayerSecond, videoPlayerThird
    }


    bool _videoPlayerRecordFirstPrepared = false;
    bool _videoPlayerRecordNoLogoFirstPrepared = false;
    bool _videoPlayerSecondPrepared = false;
    bool _videoPlayerThirdPrepared = false;

    bool _videoPlayerDemoPrepared = false;
    bool _videoPlayerDemoNoLogoPrepared = false;


    bool _sourceVideoIsPrepared = false;

    void Start()
    {
        StartCoroutine(LoadFirstVideo());
        StartCoroutine(LoadFirstNoLogoVideo());
        StartCoroutine(LoadSecondVideo());
        StartCoroutine(LoadThirdVideo());
        StartCoroutine(LoadDemoVideo());
        StartCoroutine(LoadDemoNoLogoVideo());
    }

    // Update is called once per frame
    void Update()
    {
        if (_videoPlayerRecordFirstPrepared &&
            _videoPlayerRecordNoLogoFirstPrepared &&
            _videoPlayerSecondPrepared &&
            _videoPlayerThirdPrepared &&
            _videoPlayerDemoPrepared &&
            _videoPlayerDemoNoLogoPrepared)
        {
            _sourceVideoIsPrepared = true;
        }
        else {

            _messageBoxAgent.UpdateMessageTemp("视频未准备完成");
        }
    }



    public VideoPlayer GetVideoPlayer(VideoPlayerType videoPlayerType) {
        if (!_sourceVideoIsPrepared) {
            return null;
        }

        if (videoPlayerType == VideoPlayerType.videoPlayerDemo)
        {
            return _videoPlayerDemo;
        }
        else if (videoPlayerType == VideoPlayerType.videoPlayerDemoNoLogo) {
            return _videoPlayerDemoNoLogo;
        }
        else if (videoPlayerType == VideoPlayerType.videoPlayerRecordFirst) {
            return _videoPlayerRecordFirst;
        }
        else if (videoPlayerType == VideoPlayerType.videoPlayerRecordNoLogoFirst) {
            return _videoPlayerRecordNoLogoFirst;
        }
        else if (videoPlayerType == VideoPlayerType.videoPlayerSecond)
        {
            return _videoPlayerSecond;
        }
        else if (videoPlayerType == VideoPlayerType.videoPlayerThird)
        {
            return _videoPlayerThird;
        }

        return null;
    }



    public bool IsPrepared() {
        return _sourceVideoIsPrepared;
    }



    IEnumerator LoadFirstVideo()
    {
        _videoPlayerRecordFirst.Prepare();
        while (!_videoPlayerRecordFirst.isPrepared)
        {
            yield return new WaitForSeconds(1f);
            break;

        }
        _videoPlayerRecordFirstPrepared = true;
    }

    IEnumerator LoadFirstNoLogoVideo()
    {
        _videoPlayerRecordNoLogoFirst.Prepare();
        while (!_videoPlayerRecordNoLogoFirst.isPrepared)
        {
            yield return new WaitForSeconds(1f);
            break;

        }
        _videoPlayerRecordNoLogoFirstPrepared = true;
    }



    IEnumerator LoadSecondVideo()
    {
        _videoPlayerSecond.Prepare();
        while (!_videoPlayerSecond.isPrepared)
        {
            yield return new WaitForSeconds(1f);
            break;

        }
        _videoPlayerSecondPrepared = true;
    }

    IEnumerator LoadThirdVideo()
    {
        _videoPlayerThird.Prepare();
        while (!_videoPlayerThird.isPrepared)
        {
            yield return new WaitForSeconds(1f);
            break;

        }
        _videoPlayerThirdPrepared = true;
    }

    IEnumerator LoadDemoVideo()
    {
        _videoPlayerDemo.Prepare();
        while (!_videoPlayerDemo.isPrepared)
        {
            yield return new WaitForSeconds(1f);
            break;

        }
        _videoPlayerDemoPrepared = true;
    }

    IEnumerator LoadDemoNoLogoVideo()
    {
        _videoPlayerDemoNoLogo.Prepare();
        while (!_videoPlayerDemoNoLogo.isPrepared)
        {
            yield return new WaitForSeconds(1f);
            break;

        }
        _videoPlayerDemoNoLogoPrepared = true;
    }
}