using BestHTTP;
using LitJson;
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Text;
using UnityEngine;

/// <summary>
///  Notion Api 客户端
/// </summary>
public class NotionApiClient 
{
    // 获取二维码
    //公共参数
    private int APP_ID = 99;
    private string api = "https://api.pixelplus.cc";
    private string qiniu_api = "https://upload.qiniu.com";
    private string h5_api = "https://h5.pixelplus.cc";
    private int EVENT_ID = 49;


    private int _tryTimeWhenError = 0;   // 尝试次数

    private int message_id;
    private string _token;
    private string _filePath;   // 生成的视频地址
    private string _fileUrlInCloud; // 七牛上的视频地址
    private string _fileCode;   //  文件code
    private string _emailAddress;

    bool _isMock = false;

    Action<string> _onApiError;
    Action _onMessagedIdReceived;
    Action<Texture2D> _onQRCodeReceived;

    Action _onSendEmailSuccess;
    Action<string> _onSendEmailError;


    public NotionApiClient(bool isMock, Action<string> onApiError,Action onMessageIdReceived,Action<Texture2D> onQRCodeReceived) {
        _isMock = isMock;
        _tryTimeWhenError = 0;

        _onApiError = onApiError;
        _onMessagedIdReceived = onMessageIdReceived;
        _onQRCodeReceived = onQRCodeReceived;


    }

    public void GetQrCodeIcon(string filePath) {
        _filePath = filePath;

        GetToken();
    }

    void GetToken()
    {
        HTTPRequest request = new HTTPRequest(new Uri(api + "/api/storage/qiniu/token"), HTTPMethods.Post, GetTokenRequestFinished);
        request.AddField("strategy", "default");
        request.AddField("app", "1");
        request.Send();
    }

    void GetTokenRequestFinished(HTTPRequest request, HTTPResponse response)
    {
        if (RequestIsSuccess(request, response,GetToken,"Get Token"))
        {
            JsonData data = JsonMapper.ToObject(response.DataAsText);
            if ((bool)data["status"])
            {
                //获取token请求成功
                _token = (string)data["data"]["token"];
                UploadVideo();
            }
            else
            {
                _onApiError.Invoke((string)data["message"]);
            }
        }
    }


    void UploadVideo()
    {
        HTTPRequest request = new HTTPRequest(new Uri(qiniu_api), HTTPMethods.Post, UploadVideoRequestFinished);

        try
        {
            if (_isMock)
            {
                request.AddBinaryData("file", GetVideoData(Application.dataPath + "/Out/1.mp4"));
            }
            else
            {
                request.AddBinaryData("file", GetVideoData(_filePath), "Video.mp4");
            }
        }
        catch (Exception e) {
            Debug.Log(e.Message);
            _onApiError.Invoke("File read Error !" + e.Message);

        }

        request.AddHeader("Accept", "application/json");
        request.AddField("token", _token);
        request.Send();
    }

    void UploadVideoRequestFinished(HTTPRequest request, HTTPResponse response)
    {
        if (RequestIsSuccess(request, response, UploadVideo, "Upload Video"))
        {
            JsonData data = JsonMapper.ToObject(response.DataAsText);
            if ((bool)data["status"])
            {
                _fileUrlInCloud = (string)data["data"]["url"];
                UploadUrl();
            }
            else
            {
                _onApiError.Invoke((string)data["message"]);
            }
        }
    }

    void UploadUrl()
    {
        HTTPRequest request = new HTTPRequest(new Uri(api + "/api/attachments"), HTTPMethods.Post, UploadUrlRequestFinished);
        request.AddField("app_id", APP_ID.ToString());
        request.AddField("file_url", _fileUrlInCloud);
        request.Send();
    }

    void UploadUrlRequestFinished(HTTPRequest request, HTTPResponse response)
    {
        if (RequestIsSuccess(request, response, UploadUrl, "Upload Url"))
        {
            JsonData data = JsonMapper.ToObject(response.DataAsText);

            if ((bool)data["status"])
            {
                //上传文件路径成功
                message_id = (int)data["data"]["options"]["mail"]["message"];
                _onMessagedIdReceived.Invoke();

                _fileCode = (string)data["data"]["code"];

                GetQRCode();
            }
            else
            {
                _onApiError.Invoke((string)data["message"]);
            }
        }
    }


    void GetQRCode()
    {
        HTTPRequest request = new HTTPRequest(new Uri(h5_api + "/api/wxapp/getFileQrcode/" + _fileCode + "?type=beta_2")
            , HTTPMethods.Get, GetQRCodeRequestFinished);
        request.Send();
    }

    void GetQRCodeRequestFinished(HTTPRequest request, HTTPResponse response)
    {
        if (RequestIsSuccess(request, response, GetQRCode, "Get QR Code"))
        {
            try
            {
                Texture2D r = response.DataAsTexture2D;
                _onQRCodeReceived.Invoke(r);
            }
            catch (Exception ex)
            {
                _onApiError.Invoke("Exception when transfor bytes[] to texture!");
            }
        }


    }

    public void SendEmail(string address , Action onSendEmailSuccess , Action<string> onSendEmailError) {
        _onSendEmailSuccess = onSendEmailSuccess;
        _onSendEmailError = onSendEmailError;
        _emailAddress = address;
        SendEmail();
    }

    void SendEmail()
    {

        HTTPRequest request = new HTTPRequest(new Uri(api + "/api/mail/messages/" + message_id + "/send")
            , HTTPMethods.Post, SendEmainRequestFinished);
        request.AddHeader("Content-Type", "application/json");
        request.AddHeader("Accept", "application/json");

        // 设置地址

        if (CheckEmailStrEasy(_emailAddress))
        {
            var fromJson = @"
            {
                ""to""     : [{""email"" : """ +
                    _emailAddress
                    + @"""}]}";

            request.RawData = Encoding.UTF8.GetBytes(fromJson);
            request.Send();
        }
        else
        {
            _onSendEmailError.Invoke("Email address is INVALID!");
        }

        //string address = "873074332@qq.com";

    }

    void SendEmainRequestFinished(HTTPRequest request, HTTPResponse response)
    {

        if (RequestIsSuccess(request, response, SendEmail, "Send Email"))
        {
            JsonData data = JsonMapper.ToObject(response.DataAsText);
            if ((bool)data["status"])
            {
                //发送邮件成功
                _onSendEmailSuccess.Invoke();
            }
            else {
                _onSendEmailError.Invoke((string)data["message"]);

            }
        }

    }


    private bool RequestIsSuccess(HTTPRequest request, HTTPResponse resp,Action requestAction,string errorMessageAction)
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

        // 当结果错误是，检查错误尝试次数
        if (!result)
        {
            // 当尝试次数超过3次
            if (_tryTimeWhenError == 3)
            {
                _onApiError.Invoke(errorMessageAction + " request error.");
                _tryTimeWhenError = 0;
            }
            else
            {
                _tryTimeWhenError++;
                requestAction.Invoke();
            }
        }
        else {
            _tryTimeWhenError = 0;
        }

        return result;
    }

    /// <summary>
    ///     获取字节数据
    /// </summary>
    /// <param name="path"></param>
    /// <returns></returns>
    byte[] GetVideoData(string path)
    {
        FileStream fs = new FileStream(path, FileMode.Open, FileAccess.Read);
        byte[] data = new byte[fs.Length];
        fs.Read(data, 0, data.Length);
        fs.Close();
        return data;
    }

    /// <summary>
    ///     检查输入的email
    /// </summary>
    /// <returns></returns>
    private bool CheckEmailStrEasy(string address)
    {
        if (address.Length == 0)
            return false;
        if (address.IndexOf("@") == -1)
        {
            return false;
        }
        return true;
    }
}