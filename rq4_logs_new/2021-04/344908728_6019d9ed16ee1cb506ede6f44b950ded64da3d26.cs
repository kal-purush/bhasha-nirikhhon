using System.Collections;
using System.IO;
using System.Collections.Generic;
using UnityEngine;

public class HistoryManager : MonoBehaviour
{
    // Start is called before the first frame update
    void Start()
    {
        int scoreA = 5;
        int scoreB = 2;

        string serializedData =
            "ScoreA, " + scoreA.ToString() + "\n" +
            "ScoreB, " + scoreB.ToString() + "\n";

        // Write to disk
        StreamWriter writer = new StreamWriter("MyPath.txt", true);
        writer.Write(serializedData);

        // Read
        StreamReader reader = new StreamReader("MyPath.txt");
        string lineA = reader.ReadLine();
        string[] splitA = lineA.Split(',');
        scoreA = int.Parse(splitA[1]);
    }

    // Update is called once per frame
    void Update()
    {
        
    }
}