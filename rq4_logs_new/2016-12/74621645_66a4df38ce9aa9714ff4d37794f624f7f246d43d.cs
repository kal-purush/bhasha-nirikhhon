using UnityEngine;
using System.Collections;

[System.Serializable]

public class PlayerManager {


    public string name;
    public string email;

    public PlayerManager (string name, string email)
    {
        this.name = name;
        this.email = email;
    }

}