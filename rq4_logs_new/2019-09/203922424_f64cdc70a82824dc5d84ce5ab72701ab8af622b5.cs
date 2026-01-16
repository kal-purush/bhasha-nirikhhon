using UnityEngine;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;

public class GlobalController : MonoBehaviour
{
    public static int score = 0;

    public static void SaveFile()
    {
        string destination = Application.persistentDataPath + "/save.dat";
        FileStream file;

        if (File.Exists(destination)) file = File.OpenWrite(destination);
        else file = File.Create(destination);

        score = Player.score;
        BinaryFormatter bf = new BinaryFormatter();
        bf.Serialize(file, score);
        file.Close();
    }

    public static void LoadFile()
    {
        string destination = Application.persistentDataPath + "/save.dat";
        FileStream file;

        if (File.Exists(destination)) file = File.OpenRead(destination);
        else
        {
            Debug.Log("error");
            return;
        }

        BinaryFormatter bf = new BinaryFormatter();
        score = (int) bf.Deserialize(file);
        file.Close();
    }
}