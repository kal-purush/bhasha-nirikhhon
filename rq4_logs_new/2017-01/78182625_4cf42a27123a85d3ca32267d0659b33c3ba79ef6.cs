using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class RightJoystick1 : MonoBehaviour {
    //combo
    ArrayList comboList;
    string supreme = "uldru";
    string recover = "dlurd";
    string moveUp = "lur";
    string moveDown = "rdl";
    const char up = 'u', right = 'r', down = 'd', left = 'l';
    string combo;
    string myCombo;
    //timer
    float gameStartTime;
    float curTime;
    float comboStartTime;
    float comboWaitTime;
    float comboTime;
    bool startCombo;
	// Use this for initialization
	void Start () {
        //initialize combo
        comboList = new ArrayList(10);
        comboList.Insert(0, supreme);
        comboList.Insert(1, recover);
        comboList.Insert(2, moveUp);
        comboList.Insert(3, moveDown);
        myCombo = "";
        //initialize timer
        gameStartTime = Time.time;
        comboWaitTime = 3;
        comboStartTime = Time.time;
        comboTime = 10;
        startCombo = false;
    }
	
	// Update is called once per frame
	void Update () {
        curTime = Time.time;
        //waiting
        if (!startCombo && curTime - comboStartTime >= comboWaitTime)
        {
            startCombo = true;
            randomCombo();
        }
        //start combo
        if (startCombo)
        {
            //combo succeed
            if (myCombo == combo)
            {
                release();
                comboEnd();
            }
            //combo failed
            if (curTime - comboStartTime >= comboTime)
            {
                comboEnd();
                Debug.Log("failed");
            }
        }
    }

    void comboEnd() {
        startCombo = false;
        comboStartTime = curTime;
    }

    void randomCombo()
    {
        int rand = Random.Range(0, 4);
        combo = comboList[rand].ToString();
        Debug.Log("Combo: " + combo);
    }

    void release()
    {
        if (myCombo == supreme)
        {
            transform.localScale /= 2;
            myCombo = "";
        }
        else if (myCombo == recover)
        {
            transform.localScale *= 2;
            myCombo = "";
        }
        else if (myCombo == moveUp)
        {
            transform.Translate(new Vector3(0, 0, 1000 * Time.deltaTime));
            myCombo = "";
        }
        else if (myCombo == moveDown)
        {
            transform.Translate(new Vector3(0, 0, -1000 * Time.deltaTime));
            myCombo = "";
        }

        Debug.Log("succeed");
    }

    public void TouchUp()
    {
        myCombo = "";
    }

    public void DownUp() {
        myCombo += up;
    }

    public void DownRight()
    {
        myCombo += right;
    }

    public void DownDown()
    {
        myCombo += down;
    }

    public void DownLeft()
    {
        myCombo += left;
    }
}