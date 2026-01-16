using UnityEngine;
using UnityEngine.UI;
using UnityEngine.EventSystems;

[RequireComponent(typeof(Image))]
public class DragObject : MonoBehaviour, IBeginDragHandler, IDragHandler, IEndDragHandler
{
    private Transform canvasTran;
    private GameObject draggingObject;

    private Vector2 p;
    private Vector2 size;

    public bool isDrag = true;

    public bool dragging
    {
        get;
        set;
    }

    void Awake()
    {
        canvasTran = GameObject.Find("Canvas").transform;
        p = GameObject.Find("Canvas").GetComponent<RectTransform>().rect.size / 2.0f;
        size = GetComponent<RectTransform>().rect.size / 2.0f;
        dragging = false;
    }

    public void OnBeginDrag(PointerEventData pointerEventData)
    {
        if (!isDrag)
            return;

        CreateDragObject();
        draggingObject.transform.localPosition = pointerEventData.position - p - size;
        dragging = true;
    }

    public void OnDrag(PointerEventData pointerEventData)
    {
        if (!isDrag)
            return;

        draggingObject.transform.localPosition = pointerEventData.position - p - size;
    }

    public void OnEndDrag(PointerEventData pointerEventData)
    {
        if (!isDrag)
            return;

        gameObject.GetComponent<Image>().color = Vector4.one;
        Destroy(draggingObject);
        dragging = false;
    }

    // ドラッグオブジェクト作成
    private void CreateDragObject()
    {
        draggingObject = new GameObject("Drag Object");
        draggingObject.transform.SetParent(canvasTran);
        draggingObject.transform.SetAsLastSibling();
        draggingObject.transform.localScale = Vector3.one;
        draggingObject.AddComponent<RectTransform>().pivot = Vector2.zero;

        // レイキャストがブロックされないように
        CanvasGroup canvasGroup = draggingObject.AddComponent<CanvasGroup>();
        canvasGroup.blocksRaycasts = false;

        Image draggingImage = draggingObject.AddComponent<Image>();
        Image sourceImage = GetComponent<Image>();

        draggingObject.GetComponent<Image>().sprite = GameObject.Find("DebugTx").GetComponent<Image>().sprite;
        //draggingImage.sprite = sourceImage.sprite;
        draggingImage.rectTransform.sizeDelta = sourceImage.rectTransform.sizeDelta;
        draggingImage.color = sourceImage.color;
        draggingImage.material = sourceImage.material;


        gameObject.GetComponent<Image>().color = Vector4.one * 0.6f;
    }
}