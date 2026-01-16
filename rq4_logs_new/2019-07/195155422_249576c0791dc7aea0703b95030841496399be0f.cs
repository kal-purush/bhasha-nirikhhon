
using UnityEngine;

//#if UNITY_ANDROID
//#endif

//#if UNITY_IPHONE
//#endif

public class PluginsDemo : MonoBehaviour
{

    // Use this for initialization
    void Start()
    {
#if UNITY_ANDROID
        PluginsDemoAndroidGUI.Start(gameObject);
#endif

#if UNITY_IPHONE
        PluginsDemoIosGUI.Start(gameObject);
#endif
    }


    // Update is called once per frame
    void Update()
    {

#if UNITY_ANDROID
        PluginsDemoAndroidGUI.Update();
#endif
#if UNITY_IPHONE
        PluginsDemoIosGUI.Update();
#endif
    }

    void OnGUI()
    {
#if UNITY_ANDROID
        PluginsDemoAndroidGUI.OnGUI();
#endif
#if UNITY_IPHONE
        PluginsDemoIosGUI.OnGUI();
#endif
    }

    // 开发者自定义的账号操作反回结果方法
    void AccountCallBack(string jsonStr)
    {
        Debug.Log("AccountCallBack----jsonStr-----" + jsonStr);
#if UNITY_ANDROID
        PluginsDemoAndroidGUI.CallBack(jsonStr);
#endif
#if UNITY_IPHONE
        PluginsDemoIosGUI.CallBack(jsonStr);
#endif
    }
}