using System.Collections;
using System.Collections.Generic;
using System.Linq;
using UnityEngine;
using Data;

public class ConfigData : MonoBehaviour
{
    public float[][] numArray;
    public string[][] textArray;
    private int[] num;

    private int bomNum;
    private float[] bomNumfloat = new float[]{ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
    private string[] bomNumText = { "0", "1", "2", "3", "4", "5", "6", "7", "8", "9" };
    private  float racketSize;
    private float[] racSize = { 0.8f, 1.0f, 1.5f };
    private string[] racSizeText = { "ちいさい", "ふつう", "おおきい" };
    private float racketSpeed;
    private float[] racSpeed = { 0.08f, 0.1f, 0.15f };
    private string[] racSpeedText = { "おそい", "ふつう", "はやい" }; 
    private int passScore;
    private float[] passScorefloat = { 500, 1000, 1500, 2000, 2500, 3000, 3500, 4000, 4500, 5000 };
    private string[] passScoreText = { "500", "1000", "1500", "2000", "2500", "3000", "3500", "4000", "4500", "5000" };
    private float ballSize;
    private float[] ball = { 0.8f, 1.0f, 1.2f };
    private string[] ballSizeText = { "ちいさい", "ふつう", "おおきい" };
    private float blockSize;
    private string[] blockSizeText = { "ちいさい", "ふつう", "おおきい" };
    private float[] block = { 0.8f, 1, 0f, 1.2f };
    private int timeLimit;
    private float[] time = { 30, 45, 60, 90, 120, 150, 180, 240, 300 };
    private string[] timeText = { "00:30", "00:45", "01:00", "01:30", "02:00", "02:30", "03:00", "04:00", "05:00" };


    // Start is called before the first frame update
    void Awake()
    {
        numArray = new float[][] { bomNumfloat, racSize, racSpeed, passScorefloat, ball, block, time };
        textArray = new string[][] { bomNumText, racSizeText, racSpeedText, passScoreText, ballSizeText, blockSizeText, timeText };
        num = new int[] { 1, 1, 1, 1, 1, 1, 1 };
 //       num = Enumerable.Repeat(1, 7).ToArray();
    }

    // Update is called once per frame
    void Update()
    {
        
    }

    public void setNum(configurable configurable, int plus)
    {
        int setting = num[(int)configurable];
        setting += plus;
        if(setting < 0)
        {
            setting = 0;
        }
        else if(setting > numArray[(int)configurable].Length - 1)
        {
            setting = numArray[(int)configurable].Length - 1;
        }

        num[(int)configurable] = setting;
    }

    public string getText(configurable configurable)
    {
        return textArray[(int)configurable][num[(int)configurable]];
    }

    public float getFloat(configurable configurable)
    {
        return numArray[(int)configurable][num[(int)configurable]];
    }

    public SelectableData getData()
    {
        return new SelectableData((int)numArray[0][num[0]], numArray[1][num[1]], numArray[2][num[2]], (int)numArray[3][num[3]], numArray[4][num[4]], numArray[5][num[5]], (int)numArray[6][num[6]]);
    }
}