using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.SceneManagement;

public class GraKod : MonoBehaviour
{
    private void Start()
    {
        ZwyciestwoDopasowania zwyciestwoDopasowania = transform.GetComponent<ZwyciestwoDopasowania>();
        zwyciestwoDopasowania.OnWin += Win;
    }

    public void BackToKorytarz()
    {
        SceneManager.LoadScene(Teleport.Scene.KorytarzScene.ToString());
    }

    private void Win(object sender, EventArgs e)
    {
        Debug.Log("Wygrana!");
    }
}