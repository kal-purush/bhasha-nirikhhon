using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using System.Linq;
using UnityEngine.SceneManagement;


public class PersistenceDataManager : MonoBehaviour
{
    public static PersistenceDataManager instance {get; private set;}
    private SaveData gameData;

    public List<IDataPersistence> dataPersistenceObjects;

    private void Awake(){
        if(instance != null) Debug.LogError("There are to many Persistence Data Managers.");
        instance = this;
    }

    private void Start(){
        this.dataPersistenceObjects = FindAllDataPersistenceObjects();
        LoadGame();
    }

    public void NewGame(){
        this.gameData = new SaveData();
    }

    public void SaveGame(){
        //Debug.Log("Hello 2");
        foreach(IDataPersistence dataPersistenceObj in dataPersistenceObjects){
            dataPersistenceObj.SaveData(ref gameData);
        }
    }

    public void LoadGame(){
        if(this.gameData == null){
            Debug.Log("No saved data, using starting new game");
            NewGame();
        }

        // push the loaded data to all other scripts that need it
        foreach (IDataPersistence dataPersistenceObj in dataPersistenceObjects)
        {
            dataPersistenceObj.LoadData(gameData);
        }
    }

    private List<IDataPersistence> FindAllDataPersistenceObjects()
    {

        //IEnumerable<IDataPersistence> dataPersistenceObjects = FindObjectsOfType<MonoBehaviour>(true)
        //    .OfType<IDataPersistence>();

        IEnumerable<IDataPersistence> dataPersistenceObjects = FindObjectsOfType<MonoBehaviour>().OfType<IDataPersistence>();

        return new List<IDataPersistence>(dataPersistenceObjects);
    }
}