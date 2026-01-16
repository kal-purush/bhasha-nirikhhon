using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using System.IO;
using System;

public class DataManager : MonoBehaviour
{
    public string UserDataFileName = "userData.json";

    public UserData _userData;
    public UserData userData {
        get {
            if (_userData == null) {
                LoadUserData();
                SaveUserData();
            }
            return _userData;
        }
    }

    public void LoadUserData() {
        string filePath = Application.persistentDataPath + UserDataFileName;

        if (File.Exists(filePath)) {
            Debug.Log("Load Completed!");
            string FromJsonData = File.ReadAllText(filePath);
            _userData = JsonUtility.FromJson<UserData>(FromJsonData);
        } else {
            Debug.Log("New File Completed!");

            _userData = new UserData();
        }
    }

    public void SaveUserData() {
        string ToJsonData = JsonUtility.ToJson(userData);
        string filePath = Application.persistentDataPath + UserDataFileName;
        File.WriteAllText(filePath, ToJsonData);
        Debug.Log("Save Completed!");
    }

    public void addUserData(string userName, int field, int year, float saturation) {
        _userData.addUserData(userName, field, year, saturation);
        SaveUserData();
    }
}