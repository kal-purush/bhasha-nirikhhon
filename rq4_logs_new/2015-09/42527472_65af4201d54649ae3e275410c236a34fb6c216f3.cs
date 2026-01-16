using UnityEngine;
using System.Collections;
using System.IO;
using System.Collections.Generic;

/// <summary>
/// Author: Andrew Seba
/// </summary>
public class ScriptLoad : MonoBehaviour {

    TextAsset textFile;
    TextReader reader;

    DirectoryInfo info;


    void Start()
    {
        //read in files in a directory

        info = new DirectoryInfo(Application.dataPath + "/");
        FileInfo[] levelInfo = info.GetFiles();

        foreach(FileInfo file in levelInfo)
        {
            Debug.Log(file);
        }

    }


    
	
}