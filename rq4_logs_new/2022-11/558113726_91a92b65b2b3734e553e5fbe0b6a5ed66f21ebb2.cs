using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;
using UnityEngine.EventSystems;

public class CreateProfile : MonoBehaviour
{
    public Text userName;
    public int highScore;

    public CreateProfile(Text userName, int highScore)
    {
        this.userName = userName;
        this.highScore = highScore;
    }
    public Selectable Selectable;
    // Start is called before the first frame update
    void Start()
    {
        
    }

    // Update is called once per frame
    void Update()
    {
        
    }
}