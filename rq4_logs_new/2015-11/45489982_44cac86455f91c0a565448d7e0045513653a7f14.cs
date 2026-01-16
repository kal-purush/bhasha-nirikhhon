using UnityEngine;
using System.Collections;

[System.Serializable]
public class SavedGameData
{
    public string headAccessory;
    public string bodyAccessory;
    public string footAccessory;

    public SavedGameData()
    {
        headAccessory = "None";
        bodyAccessory = "None";
        footAccessory = "None";
    }
}

public class SavedGameManager : MonoBehaviour
{
    private static SavedGameManager _instance;
    public static SavedGameManager Instance
    {
        get
        {
            if(_instance == null)
            {
                _instance = FindObjectOfType<SavedGameManager>();
                if(_instance == null)
                {
                    var go = new GameObject("SavedGameManager (new)");
                    _instance = go.AddComponent<SavedGameManager>();
                    _instance.data = new SavedGameData();
                    DontDestroyOnLoad(_instance);
                }
            }

            return _instance;
        }
    }

    private SavedGameData data;

    public SavedGameData GetData()
    {
        return data;
    }
}