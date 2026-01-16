using System.Collections;
using System.Collections.Generic;
using System;
using System.IO;
using UnityEngine;

[Serializable]
public struct SaveData
{
    public bool[] stageClearFlg;
    public float soundVolume;
    public int quality;
    public string language;
}

public class SaveManager_Y : MonoBehaviour
{
    public static SaveData sd;
    const string SAVE_FILE_PATH = "save.json";
    private bool isSaving = false;
    private bool isLoading = false;

    private void Awake()
    {
        DontDestroyOnLoad(gameObject);
    }

    private void Start()
    {
        Load();
        QualitySettings.SetQualityLevel(sd.quality);
    }

    public void Save()
    {
        if (isSaving || isLoading) return;
        isSaving = true;
        StartCoroutine(FlgReset());

        string json = JsonUtility.ToJson(sd);
#if UNITY_EDITOR
        string path = Directory.GetCurrentDirectory();
#else
        string path = AppDomain.CurrentDomain.BaseDirectory.TrimEnd('\\');
#endif
        path += ("/" + SAVE_FILE_PATH);
        StreamWriter writer = new StreamWriter(path, false);
        writer.WriteLine(json);
        writer.Flush();
        writer.Close();
    }

    public void Load()
    {
        if (isLoading || isSaving) return;
        isLoading = true;
        StartCoroutine(FlgReset());

        try
        {
#if UNITY_EDITOR
            string path = Directory.GetCurrentDirectory();
#else
            string path = AppDomain.CurrentDomain.BaseDirectory.TrimEnd('\\');
#endif
            FileInfo info = new FileInfo(path + "/" + SAVE_FILE_PATH);
            StreamReader reader = new StreamReader(info.OpenRead());
            string json = reader.ReadToEnd();
            sd = JsonUtility.FromJson<SaveData>(json);
        }
        catch (Exception e)
        {
            sd = new SaveData();
            sd.stageClearFlg = new bool[3] { false, false, false };
            sd.soundVolume = 1f;
            sd.language = "Japanese";
            sd.quality = 2;
        }
    }

    public IEnumerator FlgReset()
    {
        yield return new WaitForSeconds(0.3f);
        isSaving = false;
        isLoading = false;
    }

    public void SaveClearFlg(int stageNum)
    {
        sd.stageClearFlg[stageNum] = true;
        Save();
    }

    public void SaveSoundVolume(float vol)
    {
        sd.soundVolume = vol;
        Save();
    }

    public float GetSoundVolume()
    {
        return sd.soundVolume;
    }

    public void SaveLanguage(string lang)
    {
        sd.language = lang;
        Save();
    }

    public string GetLanguage()
    {
        return sd.language;
    }

    public void SaveQuality(int num)
    {
        sd.quality = num;
        Save();
    }
}