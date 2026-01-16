using UnityEngine;
using System.Collections.Generic;
using JAnalyticsAndroid;
using JAnalytics;

public class PluginsDemoAndroid : MonoBehaviour
{
    string str_unity = "";
	  int callbackId = 0;

    // Use this for initialization
    void Start()
    {
        gameObject.name = "Main Camera";
        JAnalyticsBindingAndroid.SetDebugMode(true);
        JAnalyticsBindingAndroid.Init();
    }

    // Update is called once per frame
    void Update()
    {
        if(Input.GetKeyUp(KeyCode.Escape) || Input.GetKeyUp(KeyCode.Home))
        {
            Application.Quit();
        }
    }

    void OnGUI()
    {
        str_unity = GUILayout.TextField(str_unity, GUILayout.Width(Screen.width - 80),
        GUILayout.Height(200));

        if (GUILayout.Button("onPageStart", GUILayout.Height(80)))
        {
            JAnalyticsBindingAndroid.OnPageStart("testOnPage-1");
        }

        if (GUILayout.Button("onPageEnd", GUILayout.Height(80)))
        {
            JAnalyticsBindingAndroid.OnPageEnd("testOnPage-1");
        }

        if (GUILayout.Button("onPurchaseEvent", GUILayout.Height(80)))
        {
            string purchaseGoodsid= "goodsId";
            string purchaseGoodsName = "篮球";
            double purchasePrice = 300;
            bool purchaseSuccess = true;
            int purchaseCurrencyUnity = Currency.CNY;
            string purchaseGoodsType = "sport";
            int purchaseGoodsCount = 1;

            Dictionary< string,string> extDic = new Dictionary<string, string>();
            extDic.Add("1", "a");
            extDic.Add("2", "b");

            JAnalyticsBindingAndroid.OnPurchaseEvent(purchaseGoodsid, purchaseGoodsName, 
                purchasePrice, purchaseSuccess, purchaseCurrencyUnity, purchaseGoodsType, purchaseGoodsCount, extDic);
        }


        if (GUILayout.Button("onBrowseEvent", GUILayout.Height(80)))
        {
            string browseId = "browse_id";
            string browseName = "深圳热点新闻";
            string browseType = "news";
            long browseDuration = 30;

            Dictionary<string, string> extDic = new Dictionary<string, string>();
            extDic.Add("1", "a");
            extDic.Add("2", "b");

            JAnalyticsBindingAndroid.OnBrowseEvent(browseId, browseName,
                browseType, browseDuration,  extDic);
        }

        if (GUILayout.Button("onRegisterEvent", GUILayout.Height(80)))
        {
            string registerMethod = "sina";
            bool registerSuccess = true;

            Dictionary<string, string> extDic = new Dictionary<string, string>();
            extDic.Add("1", "a");
            extDic.Add("2", "b");

            JAnalyticsBindingAndroid.OnRegisterEvent(registerMethod, registerSuccess, extDic);
        }

        if (GUILayout.Button("onLoginEvent", GUILayout.Height(80)))
        {
            string loginMethod = "qq";
            bool loginSuccess = true;

            Dictionary<string, string> extDic = new Dictionary<string, string>();
            extDic.Add("1", "a");
            extDic.Add("2", "b");

            JAnalyticsBindingAndroid.OnLoginEvent(loginMethod, loginSuccess, extDic);
        }

        if (GUILayout.Button("onCalculateEvent", GUILayout.Height(80)))
        {
            string eventId = "onCalculateEvent_event_id";
            double eventValue = 10.1;

            Dictionary<string, string> extDic = new Dictionary<string, string>();
            extDic.Add("1", "a");
            extDic.Add("2", "b");

            JAnalyticsBindingAndroid.OnCalculateEvent(eventId, eventValue, extDic);
        }

        if (GUILayout.Button("onCountEvent", GUILayout.Height(80)))
        {
            string eventId = "onCountEvent_event_id";

            Dictionary<string, string> extDic = new Dictionary<string, string>();
            extDic.Add("1", "a");
            extDic.Add("2", "b");

            JAnalyticsBindingAndroid.OnCountEvent(eventId, extDic);
        }


        if (GUILayout.Button("initCrashHandler", GUILayout.Height(80)))
        {
            JAnalyticsBindingAndroid.InitCrashHandler();
        }


        if (GUILayout.Button("stopCrashHandler", GUILayout.Height(80)))
        {
            JAnalyticsBindingAndroid.StopCrashHandler();
        }


        if (GUILayout.Button("identifyAccount", GUILayout.Height(80)))
        {
            Account account = new Account("AccountAccount");
            account.SetName("nnnnn");


            account.SetExtraAttr("k1", "v1");
            account.SetExtraAttr("k2", null);

            int sequence = callbackId++;
            string gameObjectNameBack = gameObject.name;
            string gameObjectMethodBack = "AccountCallBack";
            JAnalyticsBindingAndroid.IdentifyAccount(account, sequence, gameObjectNameBack, gameObjectMethodBack);
        }

        if (GUILayout.Button("detachAccount", GUILayout.Height(80)))
        {
            int sequence = callbackId++;
            string gameObjectNameBack = gameObject.name;
            string gameObjectMethodBack = "AccountCallBack";
            JAnalyticsBindingAndroid.DetachAccount(sequence, gameObjectNameBack, gameObjectMethodBack);
        }

        if (GUILayout.Button("setAnalyticsReportPeriod", GUILayout.Height(80)))
        {
            int period = 30;
            JAnalyticsBindingAndroid.SetAnalyticsReportPeriod(period);
        }

        if (GUILayout.Button("setChannel", GUILayout.Height(80)))
        {
            //string channel = null;
            string channel = "test_channel";
            JAnalyticsBindingAndroid.SetChannel(channel);
        }


        if (GUILayout.Button("Test", GUILayout.Height(80)))
        {
            Dictionary<string, string> extDic = new Dictionary<string, string>();
            extDic.Add("1", "a");
            string accountMapJson = JsonHelper.ToJson(extDic);
            extDic.Add("2", null);
            string accountMapJson2 = JsonHelper.ToJson(extDic);
            Debug.Log("accountMapJson----jsonStr-----" + accountMapJson);
            Debug.Log("accountMapJson----jsonStr2-----" + accountMapJson2);
        }
    }

    // 开发者自定义的账号操作反回结果方法
    void AccountCallBack(string jsonStr)
    {
        Debug.Log("AccountCallBack----jsonStr-----" + jsonStr);
        str_unity = jsonStr;
    }
}