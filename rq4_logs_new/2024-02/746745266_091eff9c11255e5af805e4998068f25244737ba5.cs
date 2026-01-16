using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class DataManager : Singleton<DataManager>
{
    public DataDynamic dataDynamic;
    // Start is called before the first frame update
    private void Awake()
    {
        GetPlayerpref();
        if (dataDynamic == null)
        {
            dataDynamic = new DataDynamic();
            dataDynamic.CurrentDynament = 0;
            dataDynamic.CurrentHighScore = 0;
            SetPlayerpref();
        }
    }
    public void GetPlayerpref()
    {
        string dynamicDataString = PlayerPrefs.GetString("datadynamic");
        dataDynamic = JsonUtility.FromJson<DataDynamic>(dynamicDataString);
    }
    public void SetPlayerpref()
    {
        string dynamicDataString = JsonUtility.ToJson(dataDynamic);
        PlayerPrefs.SetString("datadynamic", dynamicDataString);
    }
    private void OnApplicationQuit()
    {
        SetPlayerpref();
    }

    private void OnApplicationFocus(bool focus)
    {
        if (!focus)
        {
            SetPlayerpref();
        }
    }
}
public class DataDynamic
{
    public int CurrentDynament;
    public int CurrentHighScore;
}