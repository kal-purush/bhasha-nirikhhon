using UnityEngine;

public class StoryController : MonoBehaviour
{
    public static StoryController _instance;

    /*
    public StoryController Instance()
    {
        if (_instance == null) _instance = this;
        return _instance;
    }
    */

    private void Awake() {
        _instance = _instance ? _instance : this;
        DontDestroyOnLoad(_instance);
    }

    public void SymbolWasMatched()
    {
        Debug.Log("Symbol was matched");
        ButtonSpawner._instance.ButtonPressed(true);
    }

    public void SymbolWasMisMatched()
    {
        Debug.Log("Symbol was mismatched");
    }

    public void ReachedWinState()
    {
        Debug.Log("Reached win state");
    }

    public void ReachedLoseState()
    {
        Debug.Log("Reached lose state");
    }
}