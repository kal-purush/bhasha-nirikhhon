#if UNITY_WEBGL && !UNITY_EDITOR
using UnityEngine;

namespace WelwiseGamesSDK.Internal
{
    public class JsBridge : MonoBehaviour
    {
        public static event System.Action OnInitSuccess;
        public static event System.Action<string> OnInitError;

        public static event System.Action<string> OnGetDataSuccess;
        public static event System.Action<string> OnGetDataError;
        public static event System.Action OnSetDataSuccess;
        public static event System.Action<string> OnSetDataError;

        public static event System.Action<string> OnGetTimeSuccess;
        public static event System.Action<string> OnGetTimeError;

        public static event System.Action OnNavigateSuccess;
        public static event System.Action<string> OnNavigateError;

        public void HandleInitSuccess(string _) => OnInitSuccess?.Invoke();
        public void HandleInitError(string error) => OnInitError?.Invoke(error);

        public void HandleGetDataSuccess(string data) => OnGetDataSuccess?.Invoke(data);
        public void HandleGetDataError(string error) => OnGetDataError?.Invoke(error);

        public void HandleSetDataSuccess(string _) => OnSetDataSuccess?.Invoke();
        public void HandleSetDataError(string error) => OnSetDataError?.Invoke(error);

        public void HandleGetTimeSuccess(string timestamp) => OnGetTimeSuccess?.Invoke(timestamp);
        public void HandleGetTimeError(string error) => OnGetTimeError?.Invoke(error);

        public void HandleNavigateSuccess(string _) => OnNavigateSuccess?.Invoke();
        public void HandleNavigateError(string error) => OnNavigateError?.Invoke(error);

        private void OnDestroy()
        {
            OnInitSuccess = null;
            OnInitError = null;
            OnGetDataSuccess = null;
            OnGetDataError = null;
            OnSetDataSuccess = null;
            OnSetDataError = null;
            OnGetTimeSuccess = null;
            OnGetTimeError = null;
            OnNavigateSuccess = null;
            OnNavigateError = null;
        }
    }
}
#endif