using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class InventoryScript : MonoBehaviour
{
    public bool haveCodeRoomOne;
    public bool haveRoomOneKey;

    public GameObject codeRoomOneUI;
    public GameObject keyRoomOneUI;

    private void Update()
    {

        UIActive(haveCodeRoomOne, codeRoomOneUI);
        UIActive(haveRoomOneKey, keyRoomOneUI);

    }

    void UIActive(bool active, GameObject UIobject)
    {
        if (active) UIobject.SetActive(true);
        else UIobject.SetActive(false);
    }
}