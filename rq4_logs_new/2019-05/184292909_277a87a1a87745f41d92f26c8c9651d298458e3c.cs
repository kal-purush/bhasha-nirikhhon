using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using System;
public class LevelController : MonoBehaviour
{
    static LevelController _instance;

    static LevelController Instance
    {
        get
        {
            if (_instance == null)
            {
                _instance = FindObjectOfType<LevelController>();
                if (_instance == null)
                {
                    throw new Exception("Cannot find LevelController instance.");
                }
            }
            return _instance;
        }
    }
    void Start()
    {
        SetupLevel();
    }

    void SetupLevel(){
        SolarSystemGenerator.Instance.GenerateLevel();
        GunController.Instance.Setup();
    }
}