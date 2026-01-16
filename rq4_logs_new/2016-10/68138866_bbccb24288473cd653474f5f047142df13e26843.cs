using UnityEngine;
using System.Collections;
using System.Collections.Generic;
using System.Threading;
using UnityEngine.SceneManagement;
using UnityEngine.UI;

public class Main : MonoBehaviour
{

    // Use this for initialization
    private GameObject Red;
    private GameObject Yellow;
    private GameObject Green;
    private GameObject Blue;
    private GameObject RestartButton;
    public Text text;
    public Main0 main0;

    int onInList = 0;
    static List<int> pattern = new List<int>();
    //static int countingCorrectPattern = 1;
    bool play_Back = false;

    void Start()
    {
        RestartButton = GameObject.Find("Canvas/Restart");
        RestartButton.SetActive(false);

        Red = GameObject.Find("Canvas/Cubes/Red");
        Yellow = GameObject.Find("Canvas/Cubes/Yellow");
        Green = GameObject.Find("Canvas/Cubes/Green");
        Blue = GameObject.Find("Canvas/Cubes/Blue");
        //text =GameObject.Find("/Canvas/Score").GetComponent<Text>();
        //text.text = "score: " + pattern.Count.ToString();
        int r = Random.Range(0, 4);
        pattern.Add(r);
        
        //Debug.Log(pattern[0]);
        StartCoroutine(playBack());

        // playBack();

    }

    // Update is called once per frame
    void Update()
    {
       
        

    }

    IEnumerator playBack()
    {
        play_Back = true;
        if (pattern != null)
        {
            for (int p = 0; p < pattern.Count; p++)
            {
                if (pattern[p] == 0)
                { 
                    Red.GetComponent<Renderer>().material.color = Color.white;
                    yield return new WaitForSeconds(0.6f);
                    Red.transform.GetComponent<Renderer>().material.color = Color.red;
                    yield return new WaitForSeconds(0.3f);
                    continue;
                }
                else if (pattern[p] == 1)
                {
                    Green.GetComponent<Renderer>().material.color = Color.white;
                    yield return new WaitForSeconds(0.6f);
                    Green.GetComponent<Renderer>().material.color = Color.green;
                    yield return new WaitForSeconds(0.3f);
                    continue;
                }
                else if (pattern[p] == 2)
                {
                    Blue.GetComponent<Renderer>().material.color = Color.white;
                    yield return new WaitForSeconds(0.6f);
                    Blue.GetComponent<Renderer>().material.color = Color.blue;
                    yield return new WaitForSeconds(0.3f);
                    continue;
                }
                else if (pattern[p] == 3)
                {
                    Yellow.GetComponent<Renderer>().material.color = Color.white;
                    yield return new WaitForSeconds(0.6f);
                    Yellow.GetComponent<Renderer>().material.color = Color.yellow;
                    yield return new WaitForSeconds(0.3f);
                    continue;

                }
       
            yield return new WaitForSeconds(0.7f);
            }
            play_Back = false;
        }
    }

    public void testCorrect(int x)
    {
        if(play_Back == true)
        {
            return;
        }
        if (pattern[onInList] == x)
        {
            onInList++;
            //countingCorrectPattern++;
            Debug.Log(main0.getCountCorrectPattern());
        }
        else
        {
            Debug.Log("you lose");
            onInList = 0;
            main0.setReset();
            pattern = new List<int>();
            Red.SetActive(false);
            Blue.SetActive(false);
            Yellow.SetActive(false);
            Green.SetActive(false);
            RestartButton.SetActive(true);
            //StartCoroutine(playBack());
        }
        if(onInList >= pattern.Count)
        {
            new WaitForSeconds(1);
            int r = Random.Range(0, 4);
            pattern.Add(r);
            onInList = 0;
            main0.setCountCorrectPattern();
            if(main0.getCountCorrectPattern() > 7)
            {
                SceneManager.LoadScene("BeatThatProtoLevel2");
            }
            StartCoroutine(playBack());
            
        }
 
    }
}