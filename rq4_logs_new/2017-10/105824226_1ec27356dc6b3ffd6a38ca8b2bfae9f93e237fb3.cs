//data serialistaion 
/* save the player data 
 * in order to save to a file
 */


using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;
using UnityEngine;
using System.Text;
using System;

public class DataSerialisation : MonoBehaviour
{
    PlayerStatistics Player1;

    string path;
    


    // Use this for initialization
    void Start()
    {
        path = "JamGameData.txt";
        Player1 = new PlayerStatistics();
        saveData();
        readData();
    }

    // Update is called once per frame
    void Update()
    {
        path = "JamGameData.txt";
    }

    public void readData()
    {

        
        string rawData = "";
        //Open the stream and read it back.
        using (FileStream fs = File.OpenRead(path))
        {
            byte[] b = new byte[1024];
            UTF8Encoding temp = new UTF8Encoding(true);
            while (fs.Read(b, 0, b.Length) > 0)
            {

                rawData = temp.GetString(b);
            }

            string[] dataArray = rawData.Split('¶');

            Debug.Log(dataArray[0]);
            Debug.Log(dataArray[1]);
            Debug.Log(dataArray[2]);

        }
    }



    public void saveData()
    {
        /* delete the target file if already exist*/


        if (File.Exists(path))
        {
            Debug.Log("file did exist, deleting...");
            File.Delete(path);
        }
        else
            Debug.Log("file did not exist...");


        using (FileStream fs = File.Create(path))
        {
            /*  AddText(fs, "This is some text");
              AddText(fs, "This is some more text,");
              AddText(fs, "\r\nand this is on a new line");
              AddText(fs, "\r\n\r\nThe following is a subset of characters:\r\n");
            */

            AddText(fs, Convert.ToString(Player1.HP + "¶"));
            AddText(fs, Convert.ToString(Player1.Ammo + "¶"));
            AddText(fs, Convert.ToString(Player1.XP + "¶"));




        }
    }

    private static void AddText(FileStream fs, string value)
    {
        byte[] info = new UTF8Encoding(true).GetBytes(value);
        fs.Write(info, 0, info.Length);
    }
}


[Serializable]
public class PlayerStatistics
{
    public int SceneID = 3;

    public float HP = 75 ;
    public float Ammo = 987;
    public float XP = 17;
}