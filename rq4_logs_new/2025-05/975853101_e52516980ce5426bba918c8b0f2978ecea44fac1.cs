using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class TitleController_hara : MonoBehaviour
{
    public GameObject[] canvases; // キャンバス3つ
    public Button nextButton;
    public Button backButton;
    public Button closeButton;

    private int currentIndex = 0;

    // Start is called before the first frame update
    void Start()
    {
        UpdateCanvas();
    }

    public void Next()
    {
        if (currentIndex < canvases.Length - 1)
        {
            currentIndex++;
            UpdateCanvas();
        }
    }

    public void Back()
    {
        if (currentIndex > 0)
        {
            currentIndex--;
            UpdateCanvas();
        }
    }

    void UpdateCanvas()
    {
        // 全てのCanvasを非表示
        for (int i = 0; i < canvases.Length; i++)
        {
            canvases[i].SetActive(i == currentIndex);
        }

        // ボタン表示制御
        backButton.gameObject.SetActive(currentIndex != 0);
        nextButton.gameObject.SetActive(currentIndex != canvases.Length - 1);
        closeButton.gameObject.SetActive(currentIndex == 2);
    }
}