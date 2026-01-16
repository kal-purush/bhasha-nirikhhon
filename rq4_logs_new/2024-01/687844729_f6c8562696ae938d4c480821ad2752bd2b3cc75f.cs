using UnityEngine;

public class KeySettingListener : MonoBehaviour
{
    private static KeyCode _receivedKey = KeyCode.None;

    /// <summary>
    /// Returns key read by listener. If there is none, then the reader returns KeyCode.None.
    /// </summary>
    public static KeyCode ReceivedKey
    {
        get
        {
            KeyCode ret = _receivedKey;
            _receivedKey = KeyCode.None;

            return ret;
        }
        private set => _receivedKey = value;
    }

    private void Awake() => Disable();

    private void OnGUI()
    {
        if (Event.current.isKey && Event.current.type == EventType.KeyDown)
        {
            ReceivedKey = Event.current.keyCode;
            Disable();
        }
    }

    /// <summary>
    /// Enables the listener.
    /// </summary>
    public void Enable()
    {
        gameObject.SetActive(true);
    }
    
    /// <summary>
    /// Disables the listener.
    /// </summary>
    public void Disable()
    {
        gameObject.SetActive(false);
    }
}