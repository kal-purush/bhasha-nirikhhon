using System;
public static class CamfiServerInfo
{
    static string _serverIp = "192.168.9.67";
    static int _serverPort = 80;
    static int _serverEventPort = 8080;
    static string _serverURLStr
    {
        get { return String.Format("http://{0}:{1}", _serverIp, _serverPort); }
    }

    public static string ServerIp{get;set;}


    public static string SockIOUrlStr
    {
        get { return String.Format("http://{0}:{1}/socket.io/", _serverIp, _serverEventPort); }
    }
    public static string LiveUrlStr
    {
        get { return String.Format("{0}{1}", _serverURLStr, "/live"); }
    }
    public static string TakePictureUrlStr
    {
        get { return String.Format("{0}{1}", _serverURLStr, "/takepic/true"); }
    }
    public static string SetConfigUrlStr
    {
        get { return String.Format("{0}{1}", _serverURLStr, "/setconfigvalue"); }
    }
    public static string GetConfigUrlStr
    {
        get { return String.Format("{0}{1}", _serverURLStr, "/config"); }
    }
    public static string StartTetherUrlStr
    {
        get { return String.Format("{0}{1}", _serverURLStr, "/tether/start"); }
    }
    public static string StopTetherUrlStr
    {
        get { return String.Format("{0}{1}", _serverURLStr, "/tether/stop"); }
    }
    public static string StartLiveShowUrlStr
    {
        get { return String.Format("{0}{1}", _serverURLStr, "/capturemovie"); }
    }
    public static string StopLiveShowUrlStr
    {
        get { return String.Format("{0}{1}", _serverURLStr, "/stopcaptuovie"); }
    }
    public static string GetVersionUrlStr
    {
        get { return String.Format("{0}{1}", _serverURLStr, "/info"); }
    }
    public static string GetImagesUrlStr
    {
        get { return String.Format("{0}{1}", _serverURLStr, "/files"); }
    }



    public static string CamFiGetThumbnailByEnCodeUrlStr(string encodeUrl)
    {
        return String.Format("{0}{1}/{2}", _serverURLStr, "/thumbnail", encodeUrl);
    }
    public static string CamFiGetImageByEnCodeUrlStr(string encodeUrl)
    {
        return String.Format("{0}{1}/{2}", _serverURLStr, "/image", encodeUrl);
    }
    public static string CamFiGetRawDataByEnCodeUrlStr(string encodeUrl)
    {
        return String.Format("{0}{1}/{2}", _serverURLStr, "/raw", encodeUrl);
    }
}