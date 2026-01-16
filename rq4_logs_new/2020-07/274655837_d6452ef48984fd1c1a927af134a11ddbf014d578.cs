using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class TeleportGate : Gate
{
    float worldLeftBorderWindow, worldRightBorderWindow;
    float screenLeftBorderWindow, screenRightBorderWindow;
    RectTransform rectLeftBorder, rectRightBorder, rectWindow;

    void Start()
    {
        rectWindow = window.GetComponent<RectTransform>();
        rectLeftBorder = leftBorder.GetComponent<RectTransform>();
        rectRightBorder = rightBorder.GetComponent<RectTransform>();

        // Границы в экранном пространстве
        screenLeftBorderWindow = Screen.width * rectLeftBorder.anchorMin.x + rectWindow.rect.width / 2;
        screenRightBorderWindow = Screen.width * rectRightBorder.anchorMax.x - rectWindow.rect.width / 2;

        // Границы в мировых координатах
        worldLeftBorderWindow = Camera.main.ScreenToWorldPoint(new Vector2(screenLeftBorderWindow, 0)).x;
        worldRightBorderWindow = Camera.main.ScreenToWorldPoint(new Vector2(screenRightBorderWindow, 0)).x;

        // Увеличение размера границ в два раза (потому что идет смещение всех ворот сразу, отстальная часть за экраном)
        rectLeftBorder.SetSizeWithCurrentAnchors(RectTransform.Axis.Horizontal, rectLeftBorder.rect.width * 2);
        rectRightBorder.SetSizeWithCurrentAnchors(RectTransform.Axis.Horizontal, rectRightBorder.rect.width * 2);

        // Смещение позиции границ (для корректного отображения
        leftBorder.transform.position = new Vector2(Camera.main.ScreenToWorldPoint(new Vector2(Screen.width * rectLeftBorder.anchorMin.x, 0)).x, leftBorder.transform.position.y);
        rightBorder.transform.position = new Vector2(Camera.main.ScreenToWorldPoint(new Vector2(Screen.width * rectRightBorder.anchorMax.x, 0)).x, rightBorder.transform.position.y);

        // Модифицикация колайдеров
        leftBorder.GetComponent<BoxCollider2D>().size *= 2;
        rightBorder.GetComponent<BoxCollider2D>().size *= 2;

        Debug.Log(leftBorder.GetComponent<BoxCollider2D>().size);
        Debug.Log(rightBorder.GetComponent<BoxCollider2D>().size);
    }

    public override void goalReaction()
    {
        gameObject.transform.position = new Vector2(Random.Range(worldLeftBorderWindow, worldRightBorderWindow), gameObject.transform.position.y);
    }
}