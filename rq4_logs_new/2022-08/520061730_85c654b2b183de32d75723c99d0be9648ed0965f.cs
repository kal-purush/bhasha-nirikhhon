using System.Linq;
using System.Collections.Generic;
using UnityEngine;

public abstract class DebugMonoBehaviour : MonoBehaviour
{
    protected DebugController debugController;
    protected List<DebugTag> debugTags;

    public virtual void Awake()
    {
        debugController = DebugController.Instance;
        debugTags = new List<DebugTag>();
    }

    public virtual void Start()
    {
        // TODO remove ?
    }

    protected void LogMessage(string message)
    {
        debugController.LogMessage(message, gameObject, debugTags);
    }

    protected void LogMessage(string message, params DebugTag[] debugTags)
    {
        debugController.LogMessage(message, gameObject, debugTags.ToList());
    }

    protected void LogMessage(string message, Object context)
    {
        debugController.LogMessage(message, context, debugTags);
    }

    protected void LogMessage(string message, Object context, params DebugTag[] debugTags)
    {
        debugController.LogMessage(message, context, debugTags.ToList());
    }

    protected void LogWarning(string message)
    {
        debugController.LogWarning(message, gameObject, debugTags);
    }

    protected void LogWarning(string message, params DebugTag[] debugTags)
    {
        debugController.LogWarning(message, gameObject, debugTags.ToList());
    }

    protected void LogWarning(string message, Object context)
    {
        debugController.LogWarning(message, context, debugTags);
    }

    protected void LogWarning(string message, Object context, params DebugTag[] debugTags)
    {
        debugController.LogWarning(message, context, debugTags.ToList());
    }

    protected void LogError(string message)
    {
        debugController.LogError(message, gameObject, debugTags);
    }

    protected void LogError(string message, params DebugTag[] debugTags)
    {
        debugController.LogError(message, gameObject, debugTags.ToList());
    }

    protected void LogError(string message, Object context)
    {
        debugController.LogError(message, context, debugTags);
    }

    protected void LogError(string message, Object context, params DebugTag[] debugTags)
    {
        debugController.LogError(message, context, debugTags.ToList());
    }
}