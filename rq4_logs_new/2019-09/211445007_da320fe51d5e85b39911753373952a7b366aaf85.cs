using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class CurrentMoney : MonoBehaviour
{
    public int currentMoney;

    GameManager gm;

    public void setCurrentMoney(int currentMoney)
    {
        this.currentMoney = currentMoney;
        GetComponent<UnityEngine.UI.Text>().text = "Current Money: $" + currentMoney;
    }

    // Start is called before the first frame update
    void Start()
    {
        gm = GameObject.Find("GameManager").GetComponent<GameManager>();
    }

    // Update is called once per frame
    void Update()
    {
        
    }
}