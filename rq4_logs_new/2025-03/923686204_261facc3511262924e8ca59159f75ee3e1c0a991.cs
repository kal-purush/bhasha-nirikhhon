using UnityEngine;
using UnityEngine.Events;

[CreateAssetMenu(fileName = "BoolReference", menuName = "Variables/BoolReference")]
public class BoolReference : ScriptableObject
{
    [Header("Values")]
    [Tooltip("Value used at the start of the game or when resetting")]
    public bool initialValue;

    [SerializeField, Tooltip("Current value at runtime")]
    private bool runtimeValue;

    [Header("Debug Settings")]
    [Tooltip("If enabled, logs changes to the console")]
    public bool debugEnabled;

    public enum SaveOn
    {
        None,
        SceneLoaded
    }

    [Header("Save/Reset Options")]
    [Tooltip("When the value should be reset to 'initialValue'")]
    public SaveOn saveOn = SaveOn.SceneLoaded;

    [Tooltip("Check this if you want to reset to 'initialValue' at the specified time")]
    public bool resetToInitialValue;

    [Header("Value Changed Event")]
    [Tooltip("Event invoked whenever the runtime value changes")]
    public UnityEvent<bool> OnValueChanged;

    private void OnEnable()
    {
        // Only reset if the game is actually playing and you've enabled reset
        if (Application.isPlaying && resetToInitialValue && saveOn == SaveOn.SceneLoaded)
        {
            ResetValue();
        }
    }

    /// <summary>
    /// Access or modify the current runtime value, invokes OnValueChanged if the value changes
    /// </summary>
    public bool Value
    {
        get => runtimeValue;
        set
        {
            // If there's no actual change, do nothing
            if (runtimeValue == value) return;

            runtimeValue = value;
            OnValueChanged?.Invoke(runtimeValue);

            if (debugEnabled)
            {
                Debug.Log($"[BoolReference] Value changed to: {runtimeValue}", this);
            }
        }
    }

    /// <summary>
    /// Resets the runtime value to the initial value, triggering the OnValueChanged event if it changes
    /// </summary>
    public void ResetValue()
    {
        Value = initialValue;
    }

    /// <summary>
    /// Sets the runtime value directly, triggering the OnValueChanged event.
    /// </summary>
    public void SetValue(bool value)
    {
        Value = value;
    }

    /// <summary>
    /// Toggles the runtime value from true to false or vice versa
    /// </summary>
    public void ToggleValue()
    {
        Value = !Value;
    }
}