using UnityEngine;
using System.Collections;
using UnityEngine.EventSystems;

public class SchedulingDragHandler : MonoBehaviour
    , IBeginDragHandler, IDragHandler, IEndDragHandler
{
    public static GameObject draggingItem;
    Vector3 startPosition;
    Transform startParent;
    GameObject moveObj;

    [SerializeField]
    ScheduleType type;
    public ScheduleType Type
    {
        get
        {
            return type;
        }
    }

    public void OnBeginDrag(PointerEventData eventData)
    {
        startPosition = transform.position;
        startParent = transform.parent;
        GetComponent<CanvasGroup>().blocksRaycasts = false;

        moveObj = Instantiate<GameObject>(gameObject);
        moveObj.transform.parent = UIManager.Instance.Canvas.transform;
        moveObj.transform.localScale = Vector3.one;
        draggingItem = moveObj;
    }

    public void OnDrag(PointerEventData eventData)
    {
        moveObj.transform.position = Input.mousePosition;
    }

    public void OnEndDrag(PointerEventData eventData)
    {
        draggingItem = null;
        GetComponent<CanvasGroup>().blocksRaycasts = true;
        if (moveObj.transform.parent == startParent)
        {
            Destroy(moveObj);
        }
        moveObj = null;
    }
}