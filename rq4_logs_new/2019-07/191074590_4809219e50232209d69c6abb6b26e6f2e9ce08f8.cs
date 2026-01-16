using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.EventSystems;
using UnityEngine.UI;

public class TitleButton : MonoBehaviour
{
    [SerializeField, Header("カーソル")]
    Transform corsor;
    [SerializeField, Header("最初に選択するボタン")]
    GameObject firstSelect;
    [SerializeField, Header("イベントシステム")]
    EventSystem eventSystem;
    [SerializeField, Header("カーソルの位置")]
    Vector3 corsorPos = new Vector3(-150, 0, 0);

    GameObject selectObj;

    // Start is called before the first frame update
    void Start()
    {
        //Cursor.lockState = CursorLockMode.Locked;
        //Cursor.visible = false;
    }

    // Update is called once per frame
    void Update()
    {
        selectObj = eventSystem.currentSelectedGameObject;
        Vector3 pos = selectObj.transform.position;
        corsor.position = pos + corsorPos;
    }
}