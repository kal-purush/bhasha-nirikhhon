using UnityEngine;
using Lean.Touch;

public class RestrictMovement : MonoBehaviour
{
    public RectTransform rectTransform; // RectTransform của GameObject
    public LeanDragTranslate leanDragTranslate; // Component LeanDragTranslate
    public RectTransform canvasRect; // RectTransform của Canvas
    public Vector2 padding = new Vector2(100f, 0.1f);

    void Awake()
    {
        // Lấy các component cần thiết
        rectTransform = GetComponent<RectTransform>();
        leanDragTranslate = GetComponent<LeanDragTranslate>();
        canvasRect = GetComponentInParent<Canvas>().GetComponent<RectTransform>();
    }

    void LateUpdate()
    {
        // Lấy kích thước và vị trí của Canvas
        Vector2 canvasSize = canvasRect.sizeDelta;
        Vector2 canvasPos = canvasRect.anchoredPosition;

        // Lấy kích thước của GameObject
        Vector2 sizeDelta = rectTransform.sizeDelta * canvasRect.lossyScale.x; // Tính scale của Canvas

        // Tính toán giới hạn trong không gian Canvas
        float minX = -canvasSize.x / 2 + sizeDelta.x / 2 + padding.x + 15f;
        float maxX = canvasSize.x / 2 - sizeDelta.x / 2 - padding.x;
        float minY = -canvasSize.y / 2 + sizeDelta.y / 2f + padding.y;
        float maxY = canvasSize.y / 2 - sizeDelta.y / 2 - padding.y - 15f;

        // Lấy vị trí hiện tại của GameObject
        Vector3 currentPosition = rectTransform.anchoredPosition;

        // Giới hạn vị trí trong phạm vi Canvas
        currentPosition.x = Mathf.Clamp(currentPosition.x, minX, maxX);
        currentPosition.y = Mathf.Clamp(currentPosition.y, minY, maxY);

        // Cập nhật vị trí
        rectTransform.anchoredPosition = currentPosition;
    }
}