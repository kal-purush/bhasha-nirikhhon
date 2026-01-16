using UnityEngine;
using System.Collections;

public class Timer : MonoBehaviour 
{

    public static float seconds; // in second
    public static string timerStr;
    public static float maxSecond;

    public static string GetTimerStr()
    {
        int minute = (int)seconds/60;
        int second = (int)seconds%60;

        string timerStr = minute.ToString("00")+ ":" + second.ToString("00");

        return timerStr;
    }

    public static float GetSeconds()
    {
        return seconds;
    }

    public static void SetTimer(int seconds)
    {
        int minute = seconds/60;
        int second = seconds%60;

        timerStr = minute.ToString("00")+ ":" + second.ToString("00");
    }

    public static void IncreaseTimeRemaining(float num)
    {
        seconds = seconds + num;

        if (seconds > maxSecond)
            seconds = maxSecond;
    }

    public static void DecreaseTimeRemaining(float num)
    {
        seconds = seconds - num;
    }

}