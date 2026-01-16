using UnityEngine;
using System.Collections;
using System.Collections.Generic;
using System.Threading;
using UnityEngine.SceneManagement;
using UnityEngine.UI;
using System.IO;

public class Main0 : MonoBehaviour
{

    // Use this for initialization
    private GameObject Red;
    private GameObject Blue;
    private GameObject Yellow;
    private GameObject Green;
    private GameObject RestartButton;
    flashanimation flashfs = new flashanimation();

    Transform countscore;
    Transform playerscore;
    Transform Highscore;
    //public Text score;
    private static int count = 0;
    private static int score;



    static int onInList;
    static List<int> pattern = new List<int>();
    static int countingCorrectPattern = 1;
    bool play_Back = false;

    string[] previousscore;
    static int HighScore;
    static string nick;


    void Start()
    {
        
        Green = GameObject.Find("Canvas/Cubes/Green");
        Red = GameObject.Find("Canvas/Cubes/Red0");
        Blue = GameObject.Find("Canvas/Cubes/Blue0");
        Yellow = GameObject.Find("Canvas/Cubes/Yellow");

        

        RestartButton = GameObject.Find("Canvas/Restart");
        RestartButton.GetComponent<Button>().interactable = false;

        countscore = GameObject.Find("Canvas/CounterScore").transform;
        Highscore = GameObject.Find("Canvas/HighScore").transform;
        playerscore = GameObject.Find("Canvas/playerScore").transform;

        string line = File.ReadAllText(@"Assets\Scripts\TextFile2.txt");
        string s = File.ReadAllText(@"Assets\Scripts\TextFile1.txt");
        previousscore = line.Split(',');
       
        nick = s;

        if (previousscore.Length > 1)
        {
            int.TryParse(previousscore[1], out HighScore);
        }
        Debug.Log(HighScore);
        countscore.GetComponent<Text>().text = nick+"'s"+" Score: " + count.ToString();

        
        

        if (countingCorrectPattern <= 3)
        {
            Yellow.SetActive(false);
            Green.SetActive(false);
        }
        
        
        if (countingCorrectPattern == 1)
        {
            int r = Random.Range(0, 2);
            Debug.Log("adding to the patterns" + r);
            pattern.Add(r);
        }


        Debug.Log("count of pattern " + pattern.Count);
        atart(flashfs.playBack(pattern, Red, Blue, Green, Yellow));

    }

    // Update is called once per frame
    void Update()
    {

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
    public void notatart(IEnumerator coroutineMethod)
    {
        Debug.Log("inside notatart");
        play_Back = true;
        UnityEngine.WSA.Application.InvokeOnAppThread(() =>
        {
            StopCoroutine(coroutineMethod);
        }, false);
        play_Back = false;
    }


    public void testCorrect(int x)
    {
        Debug.Log("this is the value of onIntList " + onInList);
        Debug.Log("pattern to test: " + x);
        if (play_Back == true)
        {
            return;
        }
        if (pattern[onInList] == x)
        {
            Debug.Log("in pattern oninlist");
            onInList++;

            Debug.Log("counting correct pattern " + countingCorrectPattern);
            Debug.Log("oninlist " + onInList);
        }
        else
        {
            Debug.Log("you lose");
            onInList = 0;
            countingCorrectPattern = 1;
           
            pattern = new List<int>();
            Debug.Log("before setting active");
            Red = GameObject.Find("Canvas/Cubes/Red0");
            Blue = GameObject.Find("Canvas/Cubes/Blue0");
            Yellow = GameObject.Find("Canvas/Cubes/Yellow");
            Green = GameObject.Find("Canvas/Cubes/Green");
            Red.SetActive(false);
            Blue.SetActive(false);
            Yellow.SetActive(false);
            Green.SetActive(false);

            RestartButton = GameObject.Find("Canvas/Restart");
            RestartButton.GetComponent<Button>().interactable = true;

            Debug.Log("after setting active");

            score = count;
            Debug.Log("before getting component");
            playerscore.GetComponent<Text>().text = "Score: " + score.ToString();

            if (score > HighScore)
            {

                string s = nick+ "," +score.ToString();
                File.WriteAllText(@"Assets\Scripts\TextFile2.txt", s);
                Highscore.GetComponent<Text>().text = "New HighScore"+"\n"+ nick+"\t"+score.ToString();
            }
            else
            {
                string s = "HighScore:"+"\n"+previousscore[0] + "\t" + previousscore[1];
                Highscore.GetComponent<Text>().text = s;
            }

            Debug.Log("after getting component");
            resetScore();
        }


        if (onInList >= pattern.Count && pattern.Count != 0)
        {
            count = count + 5;
            Debug.Log("before");
            Debug.Log(count);

            Debug.Log("after");
            Debug.Log(count);
            int r;
            if (countingCorrectPattern <= 3)
            {
                r = Random.Range(0, 2);
            }
            else
            {
                r = Random.Range(0, 4);
            }
            pattern.Add(r);
            onInList = 0;
            countingCorrectPattern++;

            /*if (countingCorrectPattern > 3)
            {
                Debug.Log("counting correct over 3 " + countingCorrectPattern);
                SceneManager.LoadScene("BeatMyApp");
            }*/
            //else{
            SceneManager.LoadScene("level0");
            Debug.Log("made it past notatart and load scene and atart");
            // }   

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
    public int getScore()
    {
        return count;
    }

    public void setScore()
    {
        count = count + 5;
    }
    public void resetScore()
    {
        count = 0;
    }
    public List<int> getPattern()
    {
        return pattern;
    }
    public void resetpattern()
    {
        pattern = new List<int>();
    }

   

}