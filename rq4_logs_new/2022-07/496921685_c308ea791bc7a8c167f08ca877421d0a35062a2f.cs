using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using System;
using System.Runtime.Serialization.Formatters.Binary;
using System.IO;

public class SaveHandler
{
    private string _path;
    private BinaryFormatter _binaryFormatter;

    public SaveHandler()
    {
        _path = Application.persistentDataPath + "/SaveData.dat";
        _binaryFormatter = new BinaryFormatter();
    }
    public void Save(int points)
    {
        var file = File.Create(_path);
        var data = new SaveData();
        data._score = points;
        _binaryFormatter.Serialize(file, data);
        file.Close();
    }
    public SaveData Load()
    {
        if (!File.Exists(_path))
        {
            var defaultData = new SaveData();
            return defaultData;
        }
        var file = File.Open(_path, FileMode.Open);
        var data = (SaveData)_binaryFormatter.Deserialize(file);
        file.Close();
        return data;

    }
}
