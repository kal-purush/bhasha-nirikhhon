using System.Collections;
using System.Collections.Generic;
using System;

public class Player
{
    private int playerID;

    public List<int> icons = new List<int>();

    public List<byte> rgb = new List<byte>();

    public Player(int identification)
    {
        playerID = identification;

        Random rand = new Random(playerID);

        icons.Add(rand.Next(0, 4));
        icons.Add(rand.Next(0, 4));
        icons.Add(rand.Next(0, 4));

        rgb.Add((byte)rand.Next(0, 255));
        rgb.Add((byte)rand.Next(0, 255));
        rgb.Add((byte)rand.Next(0, 255));
        UnityEngine.Debug.Log("Player: " + playerID + "created");
    }
}