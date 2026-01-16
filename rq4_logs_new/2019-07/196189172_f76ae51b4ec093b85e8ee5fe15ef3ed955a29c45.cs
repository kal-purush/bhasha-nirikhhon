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


    bool _videoPlayerDemoIsPreparing = false;
    bool _videoPlayerDemoNoLogoIsPreparing = false;



    bool _sourceVideoIsPrepared = false;

	private bool doPreparedLock = false;
    private bool _prepareDefaultLock = false;


    void Start()
    {
		VideoPlayerType[] videoPlayerTypes = new VideoPlayerType[] { VideoPlayerType .videoPlayerDemo,
			VideoPlayerType.videoPlayerDemoNoLogo,VideoPlayerType.videoPlayerRecordFirst,
			VideoPlayerType.videoPlayerRecordNoLogoFirst,VideoPlayerType.videoPlayerSecond,
			VideoPlayerType.videoPlayerThird
		};


		for (int i = 0; i < videoPlayerTypes.Length; i++) {
			DoPreparedFirst(videoPlayerTypes[i]);
		}


        //StartCoroutine(LoadFirstVideo());
        //StartCoroutine(LoadFirstNoLogoVideo());
        //StartCoroutine(LoadSecondVideo());
        //StartCoroutine(LoadThirdVideo());
        //StartCoroutine(LoadDemoVideo());
        //StartCoroutine(LoadDemoNoLogoVideo());
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
            if (!_videoPlayerRecordFirstPrepared) {
                Debug.Log("_videoPlayerRecordFirstPrepared error");
            }

            if (!_videoPlayerRecordNoLogoFirstPrepared)
            {
                Debug.Log("_videoPlayerRecordNoLogoFirstPrepared error");
            }

            if (!_videoPlayerSecondPrepared)
            {
                Debug.Log("_videoPlayerSecondPrepared error");
            }

            if (!_videoPlayerThirdPrepared)
            {
                Debug.Log("_videoPlayerThirdPrepared error");
            }

            if (!_videoPlayerDemoPrepared)
            {
                if (!_videoPlayerDemoIsPreparing) {
                    _videoPlayerDemoIsPreparing = true;
                    DoPrepared(VideoPlayerType.videoPlayerDemo);
                }

                Debug.Log("_videoPlayerDemoPrepared error");
            }

            if (!_videoPlayerDemoNoLogoPrepared)
            {
                if (!_videoPlayerDemoNoLogoIsPreparing)
                {
                    _videoPlayerDemoNoLogoIsPreparing = true;
                    DoPrepared(VideoPlayerType.videoPlayerDemoNoLogo);
                }
                Debug.Log("_videoPlayerDemoNoLogoPrepared error");
            }

            //_messageBoxAgent.UpdateMessageTemp("视频未准备完成");
        }
    }



    public VideoPlayer GetVideoPlayer(VideoPlayerType videoPlayerType) {
        //if (!_sourceVideoIsPrepared) {
        //    return null;
        //}

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

	private void ChangePlayerPreparedStatus(VideoPlayerType videoPlayerType,bool isPrepared) {
		if (videoPlayerType == VideoPlayerType.videoPlayerDemo)
		{
			_videoPlayerDemoPrepared = isPrepared;
		}
		else if (videoPlayerType == VideoPlayerType.videoPlayerDemoNoLogo)
		{
			_videoPlayerDemoNoLogoPrepared = isPrepared;
		}
		else if (videoPlayerType == VideoPlayerType.videoPlayerRecordFirst)
		{
			_videoPlayerRecordFirstPrepared = isPrepared;
		}
		else if (videoPlayerType == VideoPlayerType.videoPlayerRecordNoLogoFirst)
		{
			_videoPlayerRecordNoLogoFirstPrepared = isPrepared;
		}
		else if (videoPlayerType == VideoPlayerType.videoPlayerSecond)
		{
			_videoPlayerSecondPrepared = isPrepared;
		}
		else if (videoPlayerType == VideoPlayerType.videoPlayerThird)
		{
			_videoPlayerThirdPrepared = isPrepared;
		}
	}



	public void PrepareDefaultPlayer() {
        if (!_prepareDefaultLock) {
            _prepareDefaultLock = true;

            VideoPlayer videoPlayer = GetVideoPlayer(VideoPlayerType.videoPlayerDemo);

            if (!videoPlayer.isPrepared)
            {
                DoPreparedFirst(VideoPlayerType.videoPlayerDemo);
            }

            VideoPlayer videoPlayer2 = GetVideoPlayer(VideoPlayerType.videoPlayerDemoNoLogo);

            if (!videoPlayer2.isPrepared)
            {
                DoPreparedFirst(VideoPlayerType.videoPlayerDemoNoLogo);
            }
        }
    }


	public void StopPlayer(VideoPlayerType videoPlayerType) {
		VideoPlayer videoPlayer = GetVideoPlayer(videoPlayerType);
		if (videoPlayer.isPlaying) {
			videoPlayer.Stop();
			ChangePlayerPreparedStatus(videoPlayerType,false);

		}

	}



	private void DoPreparedFirst(VideoPlayerType videoPlayerType)
	{

		Debug.Log("正在准备 : " + videoPlayerType);

		VideoPlayer vp = GetVideoPlayer(videoPlayerType);
		if (!vp.isPrepared)
		{
            UpdatePreparingStatus(videoPlayerType, true);
			StartCoroutine(DoPreparedIEnumerator(videoPlayerType));
		}
		
	}

	private void DoPrepared(VideoPlayerType videoPlayerType) {
		if (!doPreparedLock) {

			Debug.Log("正在准备 : " + videoPlayerType);

			doPreparedLock = true;
			VideoPlayer vp = GetVideoPlayer(videoPlayerType);
			if (!vp.isPrepared)
			{
                UpdatePreparingStatus(videoPlayerType, true);
                StartCoroutine(DoPreparedIEnumerator(videoPlayerType));
			}
		}
	}

	IEnumerator DoPreparedIEnumerator(VideoPlayerType videoPlayerType)
	{
		VideoPlayer vp = GetVideoPlayer(videoPlayerType);

		vp.Prepare();
		while (!vp.isPrepared)
		{
			yield return new WaitForSeconds(1f);
			break;

		}

		Debug.Log("准备完成 : " + videoPlayerType);
        UpdatePreparingStatus(videoPlayerType, false);


        ChangePlayerPreparedStatus(videoPlayerType, true);
		doPreparedLock = false;
	}



    private void UpdatePreparingStatus(VideoPlayerType videoPlayerType,bool isPreparing) {
        if (videoPlayerType == VideoPlayerType.videoPlayerDemo) {
            _videoPlayerDemoIsPreparing = isPreparing;
        }

        if (videoPlayerType == VideoPlayerType.videoPlayerDemoNoLogo)
        {
            _videoPlayerDemoNoLogoIsPreparing = isPreparing;
        }


    }


}