using System;
using UnityEngine;
using UnityEngine.Events;
using UnityEngine.EventSystems;

public class ShopMenuButton : MonoBehaviour, IPointerClickHandler, IPointerEnterHandler, IPointerExitHandler {
    [SerializeField]
    private OnHoverStateChangeEvent hoverEvent = new OnHoverStateChangeEvent();
    
    [SerializeField]
    private OnClickEvent clickEvent = new OnClickEvent();
    
    public void OnPointerEnter(PointerEventData eventData) {
        hoverEvent?.Invoke(true);
    }

    public void OnPointerExit(PointerEventData eventData) {
        hoverEvent?.Invoke(false);
    }

    public void OnPointerClick(PointerEventData eventData) {
        clickEvent?.Invoke();
    }
}

[Serializable]
public class OnHoverStateChangeEvent : UnityEvent<bool> { }

[Serializable]
public class OnClickEvent : UnityEvent { }