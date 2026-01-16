#if !UNITY_EDITOR && UNITY_ANDROID
#define ANDROID_BUILD
#elif !UNITY_EDITOR && UNITY_IOS
#define IOS_BUILD
#endif

using System;
using UnityEngine;

#if ANDROID_BUILD
using Unity.Notifications.Android;
#elif IOS_BUILD
using Unity.Notifications.iOS;
#endif

/// <summary>
/// ローカル通知
/// </summary>
public class LocalNotificationManager : MonoBehaviour
{
    /// <summary>
    /// 通知情報
    /// </summary>
    public class NotificationParam
    {
        public string id = "";
        public string title = "";
        public string body = "";
        public int badge = 0;
        public string data = "";
    }

#if ANDROID_BUILD
    private const string DEFAULT_CHANNEL_ID = "default";
    private const string DEFAULT_CHANNEL_NAME = "default_channel";
    private const string DEFAULT_CHANNEL_DESCRIPTION = "default_descrition";
#endif
    private const float CHECK_TIME = 1.0f;

    public NotificationParam LastOpenedNotification { get; private set; } = null;

    private float checkTimer_ = 0.0f;

    private void Awake()
    {
#if ANDROID_BUILD
        // チャンネル登録
        RegisterChannel(DEFAULT_CHANNEL_ID, DEFAULT_CHANNEL_NAME, DEFAULT_CHANNEL_DESCRIPTION);
#endif

        // コールバック設定
#if ANDROID_BUILD
        AndroidNotificationCenter.OnNotificationReceived += OnNotificationReceived;
#elif IOS_BUILD
        iOSNotificationCenter.OnNotificationReceived += OnNotificationReceived;
#endif

        // 起動時にバッジクリア
        BadgeClear();
    }

    // Update is called once per frame
    private void Update()
    {
        if (!IsActivePlatform()) { return; }

        // 開封チェック
        checkTimer_ += Time.deltaTime;
        if (checkTimer_ >= CHECK_TIME)
        {
            bool isUpdated = false;
#if ANDROID_BUILD
            AndroidNotificationIntentData data = AndroidNotificationCenter.GetLastNotificationIntent();
            if (data != null && (LastOpenedNotification == null || data.Id.ToString() != LastOpenedNotification.id))
            {
                if (LastOpenedNotification == null) { LastOpenedNotification = new NotificationParam(); }
                LastOpenedNotification.id = data.Id.ToString();
                LastOpenedNotification.title = data.Notification.Title;
                LastOpenedNotification.body = data.Notification.Text;
                LastOpenedNotification.badge = data.Notification.Number;
                LastOpenedNotification.data = data.Notification.IntentData;
                isUpdated = true;
            }
#elif IOS_BUILD
            iOSNotification notification = iOSNotificationCenter.GetLastRespondedNotification();
            if (notification != null && (LastOpenedNotification == null || notification.Identifier.ToString() != LastOpenedNotification.id))
            {
                if (LastOpenedNotification == null) { LastOpenedNotification = new NotificationParam(); }
                LastOpenedNotification.id = notification.Identifier.ToString();
                LastOpenedNotification.title = notification.Title;
                LastOpenedNotification.body = notification.Body;
                LastOpenedNotification.badge = notification.Badge;
                LastOpenedNotification.data = notification.Data;
                isUpdated = true;
            }
#endif
            // ログ表示
            if (isUpdated)
            {
                string log = "[NotificationOpened]\n";
                log += String.Format("Id : {0}\n", LastOpenedNotification.id);
                log += String.Format("Title : {0}\n", LastOpenedNotification.title);
                log += String.Format("Body : {0}\n", LastOpenedNotification.body);
                log += String.Format("Badge : {0}\n", LastOpenedNotification.badge);
                log += String.Format("Data : {0}", LastOpenedNotification.data);
                Debug.Log(log);
            }

            checkTimer_ = 0.0f;
        }
    }

    private void OnApplicationPause(bool pause)
    {
        // 復帰時にバッジクリア
        BadgeClear();
        // 開封チェック時間更新
        checkTimer_ = CHECK_TIME;
    }

    /// <summary>
    /// 有効なプラットフォームであるか
    /// </summary>
    /// <returns></returns>
    public static bool IsActivePlatform()
    {
#if ANDROID_BUILD || IOS_BUILD
        return true;
#else
        return false;
#endif
    }

    /// <summary>
    /// チャンネル登録
    /// </summary>
    /// <param name="id"></param>
    /// <param name="name"></param>
    /// <param name="description"></param>
    public static void RegisterChannel(string id, string name, string description)
    {
#if ANDROID_BUILD
        AndroidNotificationChannel channel = new AndroidNotificationChannel()
        {
            Id = id,
            Name = name,
            Importance = Importance.High,
            CanShowBadge = true,
            Description = description
        };
        AndroidNotificationCenter.RegisterNotificationChannel(channel);
#endif
    }

    /// <summary>
    /// 通知送信
    /// </summary>
    /// <param name="title">タイトル</param>
    /// <param name="message">メッセージ</param>
    /// <param name="badge">バッジ数</param>
    /// <param name="seconds">通知が届くまでの時間</param>
    /// <param name="data">データ</param>
    public static void SendNotification(string title, string message, int badge = 0, int seconds = 1, string data = "")
    {
#if ANDROID_BUILD
        SendNotification(DEFAULT_CHANNEL_ID, title, message, badge, seconds, data);
#else
        SendNotification("", title, message, badge, seconds, data);
#endif
    }

    /// <summary>
    /// 通知送信
    /// </summary>
    /// <param name="channelId">チャンネルID(Androidのみ)</param>
    /// <param name="title">タイトル</param>
    /// <param name="message">メッセージ</param>
    /// <param name="badge">バッジ数</param>
    /// <param name="seconds">通知が届くまでの時間</param>
    /// <param name="data">データ</param>
    public static void SendNotification(string channelId, string title, string message, int badge = 0, int seconds = 1, string data = "")
    {
#if ANDROID_BUILD
        // ※ secondsが2秒以上でないと開封検知できない場合がある
        if (seconds < 1) { seconds = 1; }
        AndroidNotification notification = new AndroidNotification
        {
            Title = title,
            Text = message,
            Number = badge,
            IntentData = data,
            SmallIcon = "icon_small",
            LargeIcon = "icon_large",
            ShouldAutoCancel = true,
            FireTime = DateTime.Now.AddSeconds(seconds)
        };
        AndroidNotificationCenter.SendNotification(notification, channelId);
#elif IOS_BUILD
        if (seconds < 1) { seconds = 1; }
        iOSNotification notification = new iOSNotification()
        {
            Title = title,
            Body = message,
            Badge = badge,
            Data = data,
            ShowInForeground = true,
            ForegroundPresentationOption = PresentationOption.Alert,
            Trigger = new iOSNotificationTimeIntervalTrigger()
            {
                TimeInterval = new TimeSpan(0, 0, seconds),
                Repeats = false
            }
        };
        iOSNotificationCenter.ScheduleNotification(notification);
#endif
    }

    /// <summary>
    /// 全ての通知を削除する
    /// </summary>
    public static void AllClear()
    {
#if ANDROID_BUILD
        // Androidの通知をすべて削除する
        AndroidNotificationCenter.CancelAllScheduledNotifications();
        AndroidNotificationCenter.CancelAllNotifications();
#elif IOS_BUILD
        // iOSの通知をすべて削除する
        iOSNotificationCenter.RemoveAllScheduledNotifications();
        iOSNotificationCenter.RemoveAllDeliveredNotifications();
        // バッジを削除する
        BadgeClear();
#endif
    }

    /// <summary>
    /// バッジを削除する(iOSのみ)
    /// </summary>
    public static void BadgeClear()
    {
#if IOS_BUILD
        iOSNotificationCenter.ApplicationBadge = 0;
#endif
    }

#if ANDROID_BUILD
    /// <summary>
    /// 通知検知(Android)
    /// </summary>
    /// <param name="data"></param>
    private void OnNotificationReceived(AndroidNotificationIntentData data)
    {
        string log = "[OnNotificationReceived]\n";
        log += String.Format("Id : {0}\n", data.Id);
        log += String.Format("Channel : {0}\n", data.Channel);
        log += String.Format("Title : {0}\n", data.Notification.Title);
        log += String.Format("Text : {0}\n", data.Notification.Text);
        log += String.Format("IntentData : {0}", data.Notification.IntentData);
        Debug.Log(log);
    }
#elif IOS_BUILD
    /// <summary>
    /// 通知検知(iOS)
    /// </summary>
    /// <param name="data"></param>
    private void OnNotificationReceived(iOSNotification notification)
    {
        string log = "[OnNotificationReceived]\n";
        log += String.Format("Identifier : {0}\n", notification.Identifier);
        log += String.Format("Title : {0}\n", notification.Title);
        log += String.Format("Body : {0}\n", notification.Body);
        log += String.Format("Data : {0}", notification.Data);
        Debug.Log(log);
    }
#endif
}