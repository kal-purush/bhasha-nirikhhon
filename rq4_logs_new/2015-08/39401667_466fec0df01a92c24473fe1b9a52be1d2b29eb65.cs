using UnityEngine;
using UnityEngine.UI;
using System.Collections;
using System;

public class GameTime : Text
{
    public const float SECONDS_PER_DAY = 3f;
    const string DAYTIME_TEXT = "daytime";
    const string NIGHTTIME_TEXT = "nighttime";

    void Update()
    {
        var secondsInLevel = Time.timeSinceLevelLoad;
        double day = Math.Floor(secondsInLevel / SECONDS_PER_DAY);
        string timeOfDay = day * SECONDS_PER_DAY < secondsInLevel - 0.5f * SECONDS_PER_DAY ? DAYTIME_TEXT : NIGHTTIME_TEXT;
        text = string.Format("Day {0}, {1}", day, timeOfDay);
    }
}