using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.SceneManagement;

public class SaveLoad : MonoBehaviour
{
    // Singleton
    public static SaveLoad Instance { get; private set; }
    public SaveObject saveObject;

    public void Awake()
    {
        if (Instance == null)
        {
            Instance = this;
            DontDestroyOnLoad(gameObject);
            Import(PlayerPrefs.GetString("SaveObject", ""));
        }
        else
        {
            Destroy(this.gameObject);
        }

        this.saveObject = new SaveObject();
    }

    public string Export()
    {
        string json = JsonUtility.ToJson(this.saveObject);
        PlayerPrefs.SetString("SaveObject", json);
        return json;
    }

    public void Import(string json)
    {
        this.saveObject = JsonUtility.FromJson<SaveObject>(json);
    }

    public void Load()
    {
        switch (SceneManager.GetActiveScene().name)
        {
            case "Elfendorf":
                GameObject.Find("Player").transform.position = saveObject.elfendorfPosition;
                LoadElfendorf();
                break;
            case "Ozean":
                GameObject.Find("Player").transform.position = saveObject.ozeanPosition;
                // Fix Hole
                break;
        }
    }

    public class SaveObject
    {
        // Tasks have states (0 = not started, 1 = started, 2 = finished)

        // Elfendorf
        public Vector3 elfendorfPosition;
        public int TaskBrokenBridge;
        public int TaskShoesCollected;
        public int TaskBoatOpened;

        // Ozean
        public Vector3 ozeanPosition;
        public int TaskHoleFixed;
    }

    private void LoadElfendorf()
    {
        // Broken Bridge
        if (saveObject.TaskBrokenBridge == 2)
        {
            GameObject.Find("Quest - Bridge").GetComponent<Solve>().SolveQuest();
        }
    }
}