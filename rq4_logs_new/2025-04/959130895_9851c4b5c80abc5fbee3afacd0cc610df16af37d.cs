using UnityEngine;
using System.Collections.Generic;

/// <summary>
/// Class that subscribes to Unity's log message system.
/// It listens for log messages, such as errors, warnings, and general log information.
/// Once received, the log messages are sent to the CommandController instance to be processed.
/// </summary>
public class SubscribeToUnityLogs : MonoBehaviour
{
    private CommandController CommandController;

    void OnEnable()
    {
        CommandController = GetComponent<CommandController>();

        if (CommandController == null)
        {
            Debug.LogError("CommandController not found on this GameObject.");
            return;
        } 

        Application.logMessageReceived += HandleLog;
    }

    void OnDisable()
    {
        Application.logMessageReceived -= HandleLog;
    }

    void HandleLog(string logMessage, string stackTrace, LogType logType)
    {
        if (CommandController == null)
            return;

        string timestamp = System.DateTime.Now.ToString("dd-MMM-yyyy HH:mm:ss");
        string message = $"[{timestamp}] [{logType}] {logMessage}";

        if (logType == LogType.Error || logType == LogType.Exception)
        {
            message += $"\nStack Trace: {stackTrace}";
        }

        CommandController.AddLog(message);
    }
}