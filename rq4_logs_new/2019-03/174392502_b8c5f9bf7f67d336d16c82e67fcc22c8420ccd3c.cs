using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class CompanyModel
{

    public string companyName;
    public int userCount;
    public int reputation;
    public int cash;

    public bool commEstablished = false;
    public List<string> messages;

    public CompanyModel(string companyName)
    {
        this.companyName = companyName;
        this.userCount = 0;
        this.reputation = 0;
        this.cash = 100;
        messages = new List<string>();

        ///////////
        TestData();
        ///////////
    }

    private void TestData()
    {
        messages.Add("Hei!");
        messages.Add("You: Hei!");
    }



}