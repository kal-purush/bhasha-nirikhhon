using System.Collections;
using System.Collections.Generic;
using System;
using UnityEngine.SceneManagement;
using UnityEngine.UI;
using UnityEngine;

//"Script by Brian Dang"

public class MenuStateMachine : MonoBehaviour {

    private MenuStuff source;
    private Dictionary<MenuState, Action> menu = new Dictionary<MenuState, Action>();
    private MenuState curState = MenuState.SELECT1;
    private bool SoloOn;
    public float time;
    private bool TitleDone = false;
    public AudioSource audiosource;

    enum MenuState
    {
        SELECT1,
        SELECT2,
        SELECT3,

        RACE1,
        RACE2,
        RACE3,
        RACE4,
        RACE5,

        DESIGN1,
        DESIGN2,
        DESIGN3,
        DESIGN4,
        DESIGN5,
        DESIGN6,

        RESULT,
        POSTRESULT,

        NUM_STATES
    }

	// Use this for initialization
	void Start () {
        menu.Add(MenuState.SELECT1, new Action(SelectionOne));
        menu.Add(MenuState.SELECT2, new Action(SelectionTwo));
        menu.Add(MenuState.SELECT3, new Action(SelectionThree));

        menu.Add(MenuState.RACE1, new Action(SoloSelect1));
        menu.Add(MenuState.RACE2, new Action(SoloSelect2));
        menu.Add(MenuState.RACE3, new Action(SoloSelect3));
        menu.Add(MenuState.RACE4, new Action(SoloSelect4));
        menu.Add(MenuState.RACE5, new Action(SoloSelect5));

        menu.Add(MenuState.DESIGN1, new Action(DesignSelect1));
        menu.Add(MenuState.DESIGN2, new Action(DesignSelect2));
        menu.Add(MenuState.DESIGN3, new Action(DesignDesign));
        menu.Add(MenuState.DESIGN4, new Action(DesignSave));
        menu.Add(MenuState.DESIGN5, new Action(DesignLoad));
        menu.Add(MenuState.DESIGN6, new Action(DesignReset));

        menu.Add(MenuState.RESULT, new Action(Results));
        menu.Add(MenuState.POSTRESULT, new Action(TrackResult));

        GameObject image = GameObject.Find("MenuBase");
        if (!image)
        {
            Debug.Log("ERROR: MenuBase couldn't be found.");
        }
        else
        {
            source = image.GetComponent<MenuStuff>();
        }
        source.BestTime.GetComponent<Text>().enabled = false;
        source.PTime.GetComponent<Text>().enabled = false;
        source.TrackTime.GetComponent<Text>().enabled = false;
        
        SetState(MenuState.SELECT1);
        GameObject.Find("MenuImage").GetComponent<Image>().sprite = source.StartMenu1;
        audiosource.clip = source.TitleTrack[UnityEngine.Random.Range(0, source.TitleTrack.Length)];
        audiosource.Play();
        
        
    }
    void Beep()
    {
        if (!TitleDone)
        {
            if (!audiosource.isPlaying)
            {
                audiosource.clip = source.BeepSelect;
                TitleDone = true;
            }
        }
        else if (TitleDone)
        {
            audiosource.Play();
        }
    }


    //Title Screen
    void SelectionOne()
    {
        GameObject.Find("MenuImage").GetComponent<Image>().sprite = source.StartMenu1;
        source.TrackTime.GetComponent<Text>().enabled = false;
        if (Input.GetKeyDown(KeyCode.S))
        {
            SetState(MenuState.SELECT2);
            Beep();
        }
        if (Input.GetKeyDown(KeyCode.W))
        {
            SetState(MenuState.SELECT3);
            Beep();
        }

        if (Input.GetKeyDown(KeyCode.J))
        {
            SetState(MenuState.RESULT);
            audiosource.Stop();
            audiosource.clip = source.ResultSound;
            audiosource.Play();
            time = 0.0f;
        }

        if (Input.GetKeyDown(KeyCode.Return))
        {
            SoloOn = true;
            SetState(MenuState.RACE1);
            audiosource.Stop();
            audiosource.clip = source.RaceIntro;
            audiosource.Play();
            TitleDone = false;
        }
    }

    void SelectionTwo()
    {
        GameObject.Find("MenuImage").GetComponent<Image>().sprite = source.StartMenu2;
        if (Input.GetKeyDown(KeyCode.W))
        {
            SetState(MenuState.SELECT1);
            Beep();
        }
        if (Input.GetKeyDown(KeyCode.S))
        {
            SetState(MenuState.SELECT3);
            Beep();
        }

        if (Input.GetKeyDown(KeyCode.Return))
        {
            SoloOn = false;
            SetState(MenuState.RACE1);
            audiosource.Stop();
            audiosource.clip = source.RaceIntro;
            audiosource.Play();
            TitleDone = false;
        }
    }

    void SelectionThree()
    {
        GameObject.Find("MenuImage").GetComponent<Image>().sprite = source.StartMenu3;
        if (Input.GetKeyDown(KeyCode.W))
        {
            SetState(MenuState.SELECT2);
            Beep();
        }
        if (Input.GetKeyDown(KeyCode.S))
        {
            SetState(MenuState.SELECT1);
            Beep();
        }

        if (Input.GetKeyDown(KeyCode.Return))
        {
            SetState(MenuState.DESIGN1);
        }
    }


    //Solo Race + Multi Race
    void SoloSelect1()
    {
        GameObject.Find("MenuImage").GetComponent<Image>().sprite = source.SoloMenu1;
        if (Input.GetKeyDown(KeyCode.D))
        {
            SetState(MenuState.RACE2);
            Beep();
        }
        if (Input.GetKeyDown(KeyCode.A))
        {
            SetState(MenuState.RACE5);
            Beep();
        }
        if (Input.GetKeyDown(KeyCode.Return) && SoloOn)
        {
            SceneManager.LoadScene("scenefinal");
        }
        else if (Input.GetKeyDown(KeyCode.Return) && !SoloOn)
        {
            SceneManager.LoadScene("scenefinal");
        }
    }

    void SoloSelect2()
    {
        GameObject.Find("MenuImage").GetComponent<Image>().sprite = source.SoloMenu2;
        if (Input.GetKeyDown(KeyCode.D))
        {
            SetState(MenuState.RACE3);
            Beep();
        }
        if (Input.GetKeyDown(KeyCode.A))
        {
            SetState(MenuState.RACE1);
            Beep();
        }

        if (Input.GetKeyDown(KeyCode.Return) && SoloOn)
        {
            SceneManager.LoadScene("scenefinal");
        }
        else if (Input.GetKeyDown(KeyCode.Return) && !SoloOn)
        {
            SceneManager.LoadScene("scenefinal");
        }
    }

    void SoloSelect3()
    {
        GameObject.Find("MenuImage").GetComponent<Image>().sprite = source.SoloMenu3;
        if (Input.GetKeyDown(KeyCode.D))
        {
            SetState(MenuState.RACE4);
            Beep();
        }
        if (Input.GetKeyDown(KeyCode.A))
        {
            SetState(MenuState.RACE2);
            Beep();
        }

        if (Input.GetKeyDown(KeyCode.Return) && SoloOn)
        {
            SceneManager.LoadScene("scenefinal");
        }
        else if (Input.GetKeyDown(KeyCode.Return) && !SoloOn)
        {
            SceneManager.LoadScene("scenefinal");
        }
    }

    void SoloSelect4()
    {
        GameObject.Find("MenuImage").GetComponent<Image>().sprite = source.SoloMenu4;
        if (Input.GetKeyDown(KeyCode.D))
        {
            SetState(MenuState.RACE5);
            Beep();
        }
        if (Input.GetKeyDown(KeyCode.A))
        {
            SetState(MenuState.RACE3);
            Beep();
        }

        if (Input.GetKeyDown(KeyCode.Return) && SoloOn)
        {
            SceneManager.LoadScene("scenefinal");
        }
        else if (Input.GetKeyDown(KeyCode.Return) && !SoloOn)
        {
            SceneManager.LoadScene("scenefinal");
        }
    }

    void SoloSelect5()
    {
        GameObject.Find("MenuImage").GetComponent<Image>().sprite = source.SoloMenu5;
        if (Input.GetKeyDown(KeyCode.A))
        {
            SetState(MenuState.RACE4);
            Beep();
        }
        if (Input.GetKeyDown(KeyCode.D))
        {
            SetState(MenuState.RACE1);
            Beep();
        }

        if (Input.GetKeyDown(KeyCode.Return) && SoloOn)
        {
            SceneManager.LoadScene("scenefinal");
        }
        else if (Input.GetKeyDown(KeyCode.Return) && !SoloOn)
        {
            SceneManager.LoadScene("scenefinal");
        }
    }


    //Design Menu
    void DesignSelect1()
    {
        GameObject.Find("MenuImage").GetComponent<Image>().sprite = source.DesignMenu1;
        if (Input.GetKeyDown(KeyCode.S))
        {
            SetState(MenuState.DESIGN2);
            Beep();
        }

        if (Input.GetKeyDown(KeyCode.W))
        {
            SetState(MenuState.DESIGN6);
            Beep();
        }
    }

    void DesignSelect2()
    {
        GameObject.Find("MenuImage").GetComponent<Image>().sprite = source.DesignMenu2;
        if (Input.GetKeyDown(KeyCode.S))
        {
            SetState(MenuState.DESIGN3);
            Beep();
        }

        if (Input.GetKeyDown(KeyCode.W))
        {
            SetState(MenuState.DESIGN1);
            Beep();
        }
    }

    void DesignDesign()
    {
        GameObject.Find("MenuImage").GetComponent<Image>().sprite = source.DesignMenu3;
        if (Input.GetKeyDown(KeyCode.S))
        {
            SetState(MenuState.DESIGN4);
            Beep();
        }

        if (Input.GetKeyDown(KeyCode.W))
        {
            SetState(MenuState.DESIGN2);
            Beep();
        }
    }

    void DesignSave()
    {
        GameObject.Find("MenuImage").GetComponent<Image>().sprite = source.DesignMenu4;
        if (Input.GetKeyDown(KeyCode.S))
        {
            SetState(MenuState.DESIGN5);
            Beep();
        }

        if (Input.GetKeyDown(KeyCode.W))
        {
            SetState(MenuState.DESIGN3);
            Beep();
        }
    }

    void DesignLoad()
    {
        GameObject.Find("MenuImage").GetComponent<Image>().sprite = source.DesignMenu5;
        if (Input.GetKeyDown(KeyCode.S))
        {
            SetState(MenuState.DESIGN6);
            Beep();
        }

        if (Input.GetKeyDown(KeyCode.W))
        {
            SetState(MenuState.DESIGN4);
            Beep();
        }
    }

    void DesignReset()
    {
        GameObject.Find("MenuImage").GetComponent<Image>().sprite = source.DesignMenu6;
        if (Input.GetKeyDown(KeyCode.S))
        {
            SetState(MenuState.DESIGN1);
            Beep();
        }

        if (Input.GetKeyDown(KeyCode.W))
        {
            SetState(MenuState.DESIGN5);
            Beep();
        }

        if (Input.GetKeyDown(KeyCode.Return))
        {
            SetState(MenuState.SELECT1);
        }
    }


    //Result Screen, Press "J" on Selection A to show an example Result Screen.
    void Results()
    {
        GameObject.Find("MenuImage").GetComponent<Image>().sprite = source.ResultScreen;
        source.BestTime.GetComponent<Text>().enabled = true;
        source.PTime.GetComponent<Text>().enabled = true;
        string minutes = ((int)(Time.time - Time.time) / 60).ToString();
        string seconds = (Time.time - Time.time % 60).ToString("#0000");
        source.BestTime.text = minutes + ":" + seconds;

        if (time > 6.0f)
        {
            SetState(MenuState.POSTRESULT);
        }

    }

    void TrackResult()
    {
        source.BestTime.GetComponent<Text>().enabled = false;
        source.PTime.GetComponent<Text>().enabled = false;
        source.TrackTime.GetComponent<Text>().enabled = true;
        GameObject.Find("MenuImage").GetComponent<Image>().sprite = source.TrackResult;

        if(time > 12.0f)
        {
            SetState(MenuState.SELECT1);
        }
    }


    void SetState(MenuState newState)
    {
        curState = newState;
    }
	
	void Update () {
        menu[curState].Invoke();
        time += Time.deltaTime;


    }
}