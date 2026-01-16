using System.Collections;
using System.Collections.Generic;
using System.IO;
using UnityEngine;
using UnityEngine.TextCore.Text;

public class LoadDataManager : MonoBehaviour
{
    // Singleton instance
    private static LoadDataManager _instance;
    public static LoadDataManager Instance
    {
        get
        {
            if (_instance == null)
            {
                _instance = FindObjectOfType<LoadDataManager>();
                if (_instance == null)
                {
                    GameObject singletonObject = new GameObject();
                    _instance = singletonObject.AddComponent<LoadDataManager>();
                    singletonObject.name = typeof(LoadDataManager).ToString() + " (Singleton)";
                    DontDestroyOnLoad(singletonObject);
                }
            }
            return _instance;
        }
    }


    private string saveFileName = "savedGameData.json";

    private string saveFilePath;


    private GameData gameData;

    public void Initialize()
    {

        saveFilePath = Path.Combine(Application.persistentDataPath, saveFileName);
        gameData = new GameData();
    }

    public void SaveGameData()
    {

        string jsonData = JsonUtility.ToJson(gameData);
        File.WriteAllText(saveFilePath, jsonData);

        Debug.Log("Game data saved.");
    }


    public GameData LoadGameData()
    {
        GameData loadedGameData = null;
        if (File.Exists(saveFilePath))
        {
            string jsonData = File.ReadAllText(saveFilePath);
            loadedGameData = JsonUtility.FromJson<GameData>(jsonData);
            Debug.Log("Game data loaded.");
        }
        else
        {
            Debug.LogWarning("No saved game data found.");
        }
        return loadedGameData;
    }


    public bool HasSaveData()
    {
        return File.Exists(saveFilePath);
    }


    public GameData GetGameData()
    {
        return gameData;
    }

    public void SetGameData(GameData data)
    {
        gameData = data;
    }
}