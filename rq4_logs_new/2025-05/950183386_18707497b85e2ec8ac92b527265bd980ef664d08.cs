using UnityEngine;
using WelwiseGamesSDK.Shared;

namespace WelwiseGamesSDK.Internal.Analytics
{
    internal sealed class UnityAnalytics : IAnalytics
    {
        public void SendEvent(string eventName) => Debug.Log($"[{nameof(UnityAnalytics)}] Sending event: {eventName}");
        public void SendEvent(string eventName, string data) => Debug.Log($"[{nameof(UnityAnalytics)}] Sending event: {eventName} with data: {data}");
        public void SendGameIsReady() => Debug.Log($"[{nameof(UnityAnalytics)}] Sending game is ready");
    }
}