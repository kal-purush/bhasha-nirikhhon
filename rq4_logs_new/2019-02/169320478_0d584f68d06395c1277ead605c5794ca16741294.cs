using System.Collections;
using System.Collections.Generic;
using System.IO;
using UnityEngine;

public class LevelSpawner : MonoBehaviour
{
    

    public static string[] SpawnScenes()
    {
        System.Random rnd = new System.Random();

        DirectoryInfo dir = new DirectoryInfo("Assets/Scenes/Level1");
        FileInfo[] info = dir.GetFiles("*.unity");
        
        //Derived by hardcoding but will work for now
        string[] returnArray = new string[info.Length/2];
        
        for(int i = 1; i <= returnArray.Length; i++)
        {
            List<string> tmp = new List<string>();
            foreach (FileInfo filename in info)
            {
                string name = Path.GetFileNameWithoutExtension(filename.ToString());
                if (name.Contains(i.ToString()))
                {
                    //Debug.Log(name);
                    tmp.Add(name);
                }
            }
            int r = rnd.Next(tmp.Count);

            returnArray[i - 1] = tmp[r];
        }

        return returnArray;
      
    }
}