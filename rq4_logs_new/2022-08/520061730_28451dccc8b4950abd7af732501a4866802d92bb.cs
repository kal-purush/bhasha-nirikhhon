using System.Linq;
using UnityEngine;

// This namespace is overriding the UnityEngine.Debug class, to avoid using it without this class
public class DebugController : MonoBehaviour
{
    public DebugTag[] activeDebugTags;

    public void Awake()
    {
        // This gameObject is not going to be destroyed when loading another scene
        DontDestroyOnLoad(gameObject);
        // Only log debugs if this build is a debug build
        UnityEngine.Debug.unityLogger.logEnabled = UnityEngine.Debug.isDebugBuild;
    }

    private bool IsDebugTagActive(DebugTag[] debugTags)
    {
        foreach (DebugTag debugTag in debugTags)
            if (activeDebugTags.Contains(debugTag))
                return true;
        return false;
    }

    public void LogMessage(string message, DebugTag[] debugTags)
    {
        if (IsDebugTagActive(debugTags))
            UnityEngine.Debug.Log(message);
    }

    public void LogMessage(string message, Object context, DebugTag[] debugTags)
    {
        if (IsDebugTagActive(debugTags))
            UnityEngine.Debug.Log(message, context);
    }

    public void LogWarning(string message, DebugTag[] debugTags)
    {
        if (IsDebugTagActive(debugTags))
            UnityEngine.Debug.LogWarning(message);
    }

    public void LogWarning(string message, Object context, DebugTag[] debugTags)
    {
        if (IsDebugTagActive(debugTags))
            UnityEngine.Debug.LogWarning(message, context);
    }

    public void LogError(string message, DebugTag[] debugTags)
    {
        if (IsDebugTagActive(debugTags))
            UnityEngine.Debug.LogError(message);
    }

    public void LogError(string message, Object context, DebugTag[] debugTags)
    {
        if (IsDebugTagActive(debugTags))
            UnityEngine.Debug.LogError(message, context);
    }
}