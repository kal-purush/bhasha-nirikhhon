using UnityEngine;
using System;
using System.IO;

public class Logger : MonoBehaviour
{
    public PelletHandler pelletHandler;
    public GameHandler gameHandler;
    public string filePath = "log.csv";
    private float nextTime = 0.0f;
    private GameObject ghost;

    // Start is called once before the first execution of Update after the MonoBehaviour is created
    void Start()
    {
        File.WriteAllText(filePath, "Time, Pellets, Tension, Action, Reward\n");
    }

    // Update is called once per frame
    void Update()
    {
        if(Time.time > nextTime){
            nextTime += 1.0f;
            Vector3 position = gameHandler.GetState().ghostPosition;
            string text = string.Format("{0}, {1}, {2}, ({3} {4} {5}), {6}\n", Time.time, pelletHandler.NumPellets, gameHandler.accTension, position[0], position[1], position[2], gameHandler.currReward);
            print(text);
            File.AppendAllText(filePath, text);
        }
    }
}