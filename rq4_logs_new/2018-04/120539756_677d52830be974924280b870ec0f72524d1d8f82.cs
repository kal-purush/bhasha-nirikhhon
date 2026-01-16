using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class WeaponSelection : MonoBehaviour {

    private GameObject[] _weaponList;
    private int _index;
    // Use this for initialization
    void Start () {
        _weaponList = new GameObject[transform.childCount];

        for (int i = 0; i < transform.childCount; i++)
        {
            _weaponList[i] = transform.GetChild(i).gameObject;
        }
        foreach (GameObject go in _weaponList)
        {
            go.SetActive(false);
        }

        _weaponList[0].SetActive(true);
    }

    public void Toggle(bool left)
    {
        _weaponList[_index].SetActive(false);

        if (left)
        {
            _index--;
        }
        else
        {
            _index++;
        }

        if (_index < 0)
        {
            _index = _weaponList.Length - 1;
        }
        else if (_index > _weaponList.Length - 1)
        {
            _index = 0;
        }
        _weaponList[_index].SetActive(true);
    }
    // Update is called once per frame
    void Update () {
		
	}
}