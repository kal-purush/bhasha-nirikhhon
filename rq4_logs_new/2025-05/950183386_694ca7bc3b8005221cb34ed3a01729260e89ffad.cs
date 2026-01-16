#if UNITY_WEBGL && !UNITY_EDITOR
using System;
using System.Collections.Generic;
using AOT;

namespace WelwiseGamesSDK.Internal
{
    internal static class JsLibCallbackHandler
    {
        private static readonly Dictionary<int, object> _callbackRegistry = new();
        private static int _callbackId;

        public static int RegisterCallback<T>(T callback) where T : Delegate
        {
            var id = ++_callbackId;
            _callbackRegistry[id] = callback;
            return id;
        }

        public static bool TryGetCallback<T>(int id, out T callback) where T : Delegate
        {
            if (_callbackRegistry.TryGetValue(id, out var obj) && obj is T typedCallback)
            {
                callback = typedCallback;
                return true;
            }
            callback = null;
            return false;
        }

        public static void UnregisterCallback(int id) => _callbackRegistry.Remove(id);

        [MonoPInvokeCallback(typeof(Action<int>))]
        public static void VoidCallbackHandler(int callbackId)
        {
            if (TryGetCallback<Action>(callbackId, out var callback))
            {
                callback?.Invoke();
                UnregisterCallback(callbackId);
            }
        }

        [MonoPInvokeCallback(typeof(Action<int, string>))]
        public static void StringCallbackHandler(int callbackId, string data)
        {
            if (TryGetCallback<Action<string>>(callbackId, out var callback))
            {
                callback?.Invoke(data);
                UnregisterCallback(callbackId);
            }
        }
    }
}
#endif