using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;

[Serializable]
public class UserBackup{

    public string username;
    public float userscore = 0;

    public UserBackup(string name, float score)
    {
        username = name;
        userscore = score;
    }

    public void SetName (string name)
    {
        username = name;
    }

    public bool CommitScore (float score)
    {
        if (userscore < score)
        {
            userscore = score;
            return true;
        }
        return false;
    }

}