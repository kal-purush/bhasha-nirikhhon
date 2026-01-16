using UnityEngine;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;

public class Record : MonoBehaviour
{
    public static int score = 0;
    public static bool mute = true;

    public static void SaveScore()
    {
        string destination = Application.persistentDataPath + "/score.dat";
        FileStream file;

        if (File.Exists(destination)) file = File.OpenWrite(destination);
        else file = File.Create(destination);

        score = Player.score;
        BinaryFormatter bf = new BinaryFormatter();
        bf.Serialize(file, score);
        file.Close();
    }

    public static void LoadScore()
    {
        string destination = Application.persistentDataPath + "/score.dat";
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

    public static void SaveMute()
    {
        string destination = Application.persistentDataPath + "/mute.dat";
        FileStream file;

        if (File.Exists(destination)) file = File.OpenWrite(destination);
        else file = File.Create(destination);

        mute = MainController.mute;
        BinaryFormatter bf = new BinaryFormatter();
        bf.Serialize(file, mute);
        file.Close();
    }

    public static void LoadMute()
    {
        string destination = Application.persistentDataPath + "/mute.dat";
        FileStream file;

        if (File.Exists(destination)) file = File.OpenRead(destination);
        else
        {
            Debug.Log("error");
            return;
        }

        BinaryFormatter bf = new BinaryFormatter();
        mute = (bool)bf.Deserialize(file);
        file.Close();
    }
}