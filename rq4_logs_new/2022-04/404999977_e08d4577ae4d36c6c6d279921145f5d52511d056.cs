using System.IO;
using System;
using UnityEngine;

public class RatingController : MonoBehaviour
{
    public int rating = 0;
    public GameObject door;

    public void DidntLike()
    {
        rating = 1;
        AddRating();
    }

    public void SomewhatLiked()
    {
        rating = 2;
        AddRating();
    }

    public void Like()
    {
        rating = 3;
        AddRating();
    }

    private void AddRating()
    {
        // get the evalforScene from the worldmanager
        string evalForScene = FindObjectOfType<WorldManager>().GetComponent<WorldManager>().EvalForScene;

        // Read the log files from the persistant storage
        string path = $"{Application.persistentDataPath}/{evalForScene}_analytics.log";

        // Read the log files from the persistant storage
        // Parse the json data and rewrite the file with the correct rating data
        string json = File.ReadAllText(path);
        AnalyticsData data = JsonUtility.FromJson<AnalyticsData>(json);

        data.Rating = rating;

        File.WriteAllText(path, JsonUtility.ToJson(data));

        OpenDoor();
    }

    private void OpenDoor()
    {
        door.SetActive(true);
        GameManager.instance.OnActivateDoor();
    }


}