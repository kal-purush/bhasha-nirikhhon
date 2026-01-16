using BestHTTP;
using BestHTTP.SocketIO;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using UnityEngine;
using UnityEngine.UI;

/// <summary>
///     Camera 管理器,需要持续链接
/// </summary>
public class CameraManager : MonoBehaviour
{
    private RawImage _liveScreen;

    SocketManager socketioManager;

    Action _onConnectCamfiAction;
    Action _onBreakCamfiAction;
    Action _onConnectCameraAction;
    Action _onBreakCameraAction;
    Action _onConnectLiveShowAction;
    Action _onBreakLiveShowAction;
    Action<byte[],Texture2D> _onFileAddAction;
    Action<string> _onFileAddErrorAction;
    Action<string> _onCameraManagerError;

    private bool _connectCamfiSuccess = false;  // Cam-fi 连接状态
    private bool _connectCameraSuccess = false; //  相机 连接状态
    private bool _connectLiveShowSuccess = false;   //  实时取景  连接状态

    VideoReceiver _videoReceiver;

    private bool _currentFrameLock = false;
    private bool _showLive = false;


    #region 【公开】 初始化相机状态

    /// <summary>
    ///     初始化相机状态
    /// </summary>
    public void Init(Action onConnectCamfi,Action onConnectCamfiFailed,
        Action onConnectCamera,Action onConnectCameraFailed) {
        _onConnectCamfiAction = onConnectCamfi;
        _onBreakCamfiAction = onConnectCamfiFailed;
        _onConnectCameraAction = onConnectCamera;
        _onBreakCameraAction = onConnectCameraFailed;

        socketioManager = new SocketManager(new Uri(CamfiServerInfo.SockIOUrlStr));
        InitSocketIOManager();

        try
        {
            socketioManager.Open();
        }
        catch (Exception e)
        {
            _onCameraManagerError.Invoke(e.Message);
        }
    }

    #endregion


    #region 【公开】 初始化实时取景


    /// <summary>
    ///     初始化实施取景
    /// </summary>
    /// <param name="onConnectLiveShowSuccess"></param>
    /// <param name="onConnectLiveShowFailed"></param>
    public void InitLiveShow(Action onConnectLiveShowSuccess, Action onConnectLiveShowFailed) {
        _onConnectLiveShowAction = onConnectLiveShowSuccess;
        _onBreakLiveShowAction = onConnectLiveShowFailed;
        InitLiveView();
    }

    #endregion


    #region 【公开】 显示实时

    /// <summary>
    ///     显示实时
    /// </summary>
    public void ShowLive() {
        _showLive = true;
    }

    #endregion

    #region 【公开】 关闭实时

    /// <summary>
    ///     关闭实时
    /// </summary>
    public void CloseLive() {
        _showLive = false;
    }
    #endregion


    #region 【公开】 断开实时取景

    /// <summary>
    ///     断开实时取景
    /// </summary>
    public void DisconnectLiveShow()
    {
        _videoReceiver.Close();

        HTTPRequest request = new HTTPRequest(new Uri(CamfiServerInfo.StopLiveShowUrlStr), HTTPMethods.Get, onRequestCallback);
        request.Send();
    }

    #endregion



    /// <summary>
    ///     运行
    /// </summary>
    public void Run()
    {


    }

    #region 【公开】 设置实时取景框

    public void SetLiveScreen(RawImage screen)
    {
        _liveScreen = screen;
    }

    #endregion


    #region 【公开】 拍摄

    public void Shoot(Action<byte[], Texture2D> onShootSuccess,Action<string> onShootError)
    {
        _onFileAddAction = onShootSuccess;
        _onFileAddErrorAction = onShootError;

        HTTPRequest request = new HTTPRequest(new Uri(CamfiServerInfo.TakePictureUrlStr), HTTPMethods.Get, onRequestCallback);
        request.Send();
    }
    #endregion





    /// <summary>
    ///     初始化实时取景
    /// </summary>
    private void InitLiveView()
    {
        Debug.Log("尝试实时取景！");

        if (!_connectCameraSuccess)
        {
            Debug.Log("相机未正常连接，无法取景");
            return;
        }

        HTTPRequest request = new HTTPRequest(new Uri(CamfiServerInfo.StartLiveShowUrlStr), HTTPMethods.Get, onCaptureMovie);
        request.Send();
    }

    /// <summary>
    /// 启动实时取景视频流。调用这个API，会创建一个TCP服务器，端口为890.客户端可以通过SOCKET端口，读取视频流。视频流格式为MJPEG. 
    /// 返回: 成功返回 状态代码 200 OK，否则返回状态代码500.
    /// </summary>
    /// <param name="request"></param>
    /// <param name="response"></param>
    void onCaptureMovie(HTTPRequest request, HTTPResponse response)
    {
        if (RequestIsSuccess(request, response))
        {
            if (response.StatusCode == 200)
            {
                VideoReceiver videoReceiver = new VideoReceiver(onVideoFramePrepared);
                videoReceiver.Receive();
            }
            else
            {
                // 实时取景失败
                _connectLiveShowSuccess = false;
                _onConnectLiveShowAction.Invoke();
            }
        }
        else
        {
            // 连接实时取景失败
            _connectLiveShowSuccess = false;
            _onBreakLiveShowAction.Invoke();
        }
    }

    void onVideoFramePrepared(byte[] content)
    {
        if (!_currentFrameLock)
        {
            _currentFrameLock = true;
            if (_showLive) {
                ScreenFactoryInvoker.AddCommand(new VideoDecodeTask(content, _liveScreen));
            }
            _currentFrameLock = false;

        }
    }







    #region Socket 回调
    private void InitSocketIOManager()
    {
        socketioManager.Socket.On("connect", OnConnected);
        socketioManager.Socket.On("disconnect", OnDisConnected);
        socketioManager.Socket.On("camera_remove", OnCameraRemove);
        socketioManager.Socket.On("camera_add", OnCameraAdd);
        socketioManager.Socket.On("file_added", OnFileAdded);

    }
    #endregion



    #region CamFi服务器SocketIO事件回调函数
    void OnConnected(BestHTTP.SocketIO.Socket socket, Packet packet, params object[] args)
    {
        Debug.LogWarning("与CamFi的SokectIO连接建立");
        _connectCamfiSuccess = true;
        _onConnectCamfiAction.Invoke();


    }
    void OnDisConnected(BestHTTP.SocketIO.Socket socket, Packet packet, params object[] args)
    {
        Debug.LogWarning("与CamFi的SokectIO连接 断开");
        _connectCamfiSuccess = false;
        _onBreakCamfiAction.Invoke();


    }
    void OnCameraAdd(BestHTTP.SocketIO.Socket socket, Packet packet, params object[] args)
    {        
        Debug.LogWarning("CamFi连接了相机");
        _connectCameraSuccess = true;
        _onConnectCameraAction.Invoke();
    }

    void OnCameraRemove(BestHTTP.SocketIO.Socket socket, Packet packet, params object[] args)
    {
        Debug.LogWarning("CamFi丢失相机连接");
        _connectCameraSuccess = false;
        _onBreakCameraAction.Invoke();

    }
    void OnFileAdded(BestHTTP.SocketIO.Socket socket, Packet packet, params object[] args)
    {
        string pathInCamfi = args[0].ToString();
        Debug.Log("照片添加，其Url: " + pathInCamfi);
        HTTPRequest request = new HTTPRequest(new Uri(CamfiServerInfo.CamFiGetRawDataByEnCodeUrlStr(UrlEncode(pathInCamfi))), HTTPMethods.Get, GetRawDataFinished);
        //request.Send();

    }
    void OnLiveshowError(BestHTTP.SocketIO.Socket socket, Packet packet, params object[] args)
    {
        Debug.LogWarning("实时取景发送错误");
    }
    void OnLiveshowData(BestHTTP.SocketIO.Socket socket, Packet packet, params object[] args)
    {
        //print(packet.AttachmentCount);
    }

    /// <summary>
    ///     获取原图
    /// </summary>
    /// <param name="request"></param>
    /// <param name="response"></param>
    void GetRawDataFinished(HTTPRequest request, HTTPResponse response)
    {
        if (response.StatusCode == 200) { 

            //获取照片成功
            Debug.Log("获取照片成功");
            _onFileAddAction.Invoke(response.Data, response.DataAsTexture2D);
        }
        else
        {
            Debug.Log("获取照片失败：" + response.StatusCode + response.Data);
            _onFileAddErrorAction.Invoke(response.DataAsText);
        }
    }


    #endregion


    void onRequestCallback(HTTPRequest request, HTTPResponse response)
    {
    }



    private bool RequestIsSuccess(HTTPRequest request, HTTPResponse resp)
    {
        bool result = false;
        switch (request.State)
        {
            // The request finished without any problem.
            case HTTPRequestStates.Finished:
                result = true;
                break;

            // The request finished with an unexpected error. The request's Exception property may contain more info about the error.
            case HTTPRequestStates.Error:
                Debug.Log("HTTPRequestStates Error ");
                break;

            // The request aborted, initiated by the user.
            case HTTPRequestStates.Aborted:
                Debug.LogWarning("Request Aborted!");
                break;

            // Connecting to the server is timed out.
            case HTTPRequestStates.ConnectionTimedOut:
                Debug.LogError("Connection Timed Out!");
                break;

            // The request didn't finished in the given time.
            case HTTPRequestStates.TimedOut:
                Debug.LogError("Processing the request Timed Out!");
                break;
        }
        return result;
    }

    /// <summary>
    /// $filepath 是根据list命令中返回的照片路径.传入时需要进行url编码。编码格式如下： %2Fstorage001%2FDCIM%2Fxxxx1.jpg
    /// </summary>
    /// <param name="str"></param>
    /// <returns></returns>
    public static string UrlEncode(string str)
    {
        StringBuilder sb = new StringBuilder();
        byte[] byStr = System.Text.Encoding.UTF8.GetBytes(str); //默认是System.Text.Encoding.Default.GetBytes(str)
        for (int i = 0; i < byStr.Length; i++)
        {
            sb.Append(@"%" + Convert.ToString(byStr[i], 16));
        }

        return (sb.ToString());
    }
}