using System.Collections.Generic;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;
using UnityEngine;
using System.Runtime.Serialization;

public class SaveManager:MonoBehaviour
{
    private DataController dataController;    // the Dictionary used to save and load data to/from disk

    private void Awake()
    {
        dataController = GameObject.FindObjectOfType<DataController>();
    }

    public void saveDataToDisk(string filename)
    {
        string path = Application.persistentDataPath + "/" + filename;
        FileStream file = new FileStream(path, FileMode.OpenOrCreate);
        try
        {
            BinaryFormatter binaryFormatter = new BinaryFormatter();
            binaryFormatter.Serialize(file, dataController.getScenesToQuestions());
            file.Close();
        }
        catch (SerializationException e)
        {
            Debug.LogError("There was an issue serializing this data: " + e.Message);
        }
        finally {
            file.Close();
        }
    }

    public bool loadDataFromDisk(string filename)
    {
        bool isDataLoaded = false;
        string path = Application.persistentDataPath + "/" + filename;
        if (File.Exists(path))
        {
            FileStream file = new FileStream(path, FileMode.Open);
            Dictionary<string, List<Question>> dictSceneQuestion;
            try
            {
                BinaryFormatter binaryFormatter = new BinaryFormatter();
                dictSceneQuestion = (Dictionary<string, List<Question>>)binaryFormatter.Deserialize(file);
                dataController.setScenesToQuestions(dictSceneQuestion);
                isDataLoaded = true;
                return isDataLoaded;
            }
            catch (SerializationException e)
            {
                Debug.LogError("There was an issue deserializing this data: " + e.Message);
                return isDataLoaded;
            }
            finally
            {
                file.Close();
            }
        }
    else{
            Debug.Log("File does not exist");
            return isDataLoaded;
        }    
    }

    public bool deleteData(string filename)
    {
        bool isFileExist = false;
        string path = Application.persistentDataPath + "/" + filename;
        if (!File.Exists(path))
        {
            Debug.Log("File does not exist");
            return isFileExist;
        }
        else
        {
            try
            {
                File.Delete(path);
                Debug.Log("Data deleted from: " + path.Replace("/", "\\"));
                isFileExist = true;
            }
            catch (SerializationException e)
            {
                Debug.LogWarning("Failed To Delete Data: " + e.Message);
            }
            return isFileExist;
        }
    }

}