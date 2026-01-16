using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class AchievementManager : MonoBehaviour {
    public GameObject[] _lock = new GameObject[6];
	// Use this for initialization
	void Start () {
		
	}
    private void OnEnable()
    {
        string AchieveName;
        int num;
        for (int i = 0; i < _lock.Length; i++)
        {
            num = i + 1;
            AchieveName = "Achieve_" + num;
            if (PlayerPrefs.GetInt(AchieveName) == 1)
                _lock[i].SetActive(false);
            else
                _lock[i].SetActive(true);
        }
    }
}
    