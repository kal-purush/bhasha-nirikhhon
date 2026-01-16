using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;
using TMPro;

public class DifficultyController : MonoBehaviour
{
    private TMP_Dropdown difficultyDropdown;

    void Start()
    {
        difficultyDropdown = GetComponent<TMP_Dropdown>();
        difficultyDropdown.value = PlayerPrefs.GetInt("difficulty");

    }

    public void changeDifficulty()
    {
        PlayerPrefs.SetInt("difficulty", difficultyDropdown.value);
        difficultyDropdown.value = PlayerPrefs.GetInt("difficulty");
    }
}