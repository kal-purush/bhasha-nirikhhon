using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class AlbumFTUE : MonoBehaviour
{
    int stepIdx;
    [SerializeField] private GameObject[] steps;
    private string d_pad_h;
    private string d_pad_v;
    private string b_btn;
    private bool d_pad_h_Registered;
    private bool d_pad_v_Registered;
    private bool b_btn_Registered;

    // Start is called before the first frame update
    void Start()
    {
        switch (Application.platform)
        {
            case RuntimePlatform.WindowsPlayer:
            case RuntimePlatform.WindowsEditor:
                d_pad_h = "DPAD_h Windows";
                d_pad_v = "DPAD_v Windows";
                b_btn = "Xbox_B_Button";
                break;
        }
        stepIdx = 0;
        GameManager.Instance.currentGameSate = GameManager.GameState.AlbumFTUE;
        steps[stepIdx].SetActive(true);
        d_pad_h_Registered = false;
        d_pad_v_Registered = false;
        b_btn_Registered = false;
    }

    // Update is called once per frame
    void Update()
    {
        if (stepIdx == 0)
        {
            if (Input.GetAxis(d_pad_v) == 1) // open album
            {
                if (!d_pad_v_Registered)
                {
                    d_pad_v_Registered = true;
                    StartCoroutine(NextStep());
                }
            }
        }
        if (stepIdx == 1)
        {
            if (Input.GetAxis(d_pad_h) == 1 || Input.GetAxis(d_pad_h) == -1) // switch pages
            {
                if (!d_pad_h_Registered)
                {
                    d_pad_h_Registered = true;
                    StartCoroutine(NextStep());
                }
            }
        }
        if (stepIdx == 2)
        {
            if (Input.GetAxis(b_btn) == 1) // close album
            {
                if (!b_btn_Registered)
                {
                    b_btn_Registered = true;
                    StartCoroutine(NextStep());
                }
            }
        }
    }

    IEnumerator NextStep()
    {
        yield return new WaitForSeconds(3f);
        steps[stepIdx].SetActive(false);
        stepIdx++;
        steps[stepIdx].SetActive(true);
        if (stepIdx == steps.Length - 1)
        {
            PlayerPrefs.SetInt("albumFTUEComplete", 1);
            yield return new WaitForSeconds(3f);
            steps[stepIdx].SetActive(false);
            FTUEManager.Instance.CheckFTUE();
        }
    }
}