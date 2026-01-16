using UnityEngine;
using System.Collections;
using System.Collections.Generic;
using System.Threading;
using UnityEngine.SceneManagement;
using UnityEngine.UI;

public class Main0 : MonoBehaviour
{

    // Use this for initialization
    private GameObject Red;
    private GameObject Blue;
    private GameObject RestartButton;
    private  GameObject CounterScore;
    flashanimation flashfs = new flashanimation();

    //________________________
    //reggies counting score for display during game
    Transform countscore;
    //public Text score;
    private static int count;
    //private bool updatescore;
    //_________________________

    static int onInList;
    static List<int> pattern = new List<int>();
    static int countingCorrectPattern = 1;
    bool play_Back = false;


    

    void Start()
    {
        RestartButton = GameObject.Find("Canvas/Restart");
        RestartButton.SetActive(false);

        

        //________________________________
        count = 0;
        //updatescore = false;
        countscore = GameObject.Find("Canvas/CounterScore").transform;
        countscore.GetComponent<Text>().text = "Score: " + count.ToString();
        //________________________________

        Red = GameObject.Find("Canvas/Cubes/Red0");
        //score = countScore.text;
        Blue = GameObject.Find("Canvas/Cubes/Blue0");
        if (countingCorrectPattern == 1)
        {
            int r = Random.Range(0, 2) * 2;
            Debug.Log("adding to the patterns" + r);
            pattern.Add(r);
        }

        Debug.Log("count of pattern " + pattern.Count);

        //Debug.Log(pattern[0]);
        //atart();
        atart(flashfs.playBack(pattern, Red, Blue));
        // playBack();

    }

    // Update is called once per frame
    void Update()
    {
       /* if (updatescore == true)
        {
            count = count + 5;
            countscore = GameObject.Find("Canvas/CounterScore").transform;
            countscore.GetComponent<Text>().text = "Score: " + count.ToString();
            updatescore = false;
        }*/
    }

    public void atart(IEnumerator coroutineMethod)
    {
        play_Back = true;
        UnityEngine.WSA.Application.InvokeOnAppThread(() =>
        {
            StartCoroutine(coroutineMethod);
        }, false);
        play_Back = false;
    }


    public void testCorrect(int x)
    {
        Debug.Log("this is the value of onIntList" + onInList);
        Debug.Log("pattern to test: " + x);
        Debug.Log("this is the pattern on the list: " + pattern[onInList]);
        if (play_Back == true)
        {
            return;
        }
        if (pattern[onInList] == x)
        {
            Debug.Log("in pattern oninlist");
            onInList++;
            //___________________
            //updatescore = true;
           // Update();
            //__________________
           
            //countingCorrectPattern++;
            Debug.Log("counting correct pattern " + countingCorrectPattern);
            Debug.Log("oninlist " + onInList);
        }
        else
        {
            Debug.Log("you lose");
            onInList = 0;
            countingCorrectPattern = 1;

            pattern = new List<int>();
            Red.SetActive(false);
            Blue.SetActive(false);
            //Yellow.SetActive(false);
            //Green.SetActive(false);
            //___________________________
            RestartButton.SetActive(true);
            
            //___________________________

            //StartCoroutine(playBack());
        }


        if (onInList >= pattern.Count && pattern.Count != 0)
        {
            count = count + 5;
            countscore = GameObject.Find("Canvas/CounterScore").transform;
            countscore.GetComponent<Text>().text = "Score: " + count.ToString();
            int r = Random.Range(0, 2)*2;
            pattern.Add(r);
            onInList = 0;
            countingCorrectPattern++;
            if(countingCorrectPattern >3)
            {
                Debug.Log("counting correct over 3 " + countingCorrectPattern);
                SceneManager.LoadScene("BeatMyApp");
            }
            
            else{
                Debug.Log("the number of correct pattern less than 4 " + countingCorrectPattern);

                SceneManager.LoadScene("level0");
                // atart(flashfs.playBack(pattern, Red, Blue));
                
            }   

        }

    }

    public int getCountCorrectPattern()
    {
        return countingCorrectPattern;
    }

    public void setCountCorrectPattern()
    {
        countingCorrectPattern++;
    }
    public void setReset()
    {
        countingCorrectPattern = 1;
    }
}