using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.Serialization;

public class SettingManager : MonoBehaviour
{
    [SerializeField] Language _settingLanguage;
    
}

enum  Language
{
    English,
    Japanese,
    Chinese
}