using System;
using UnityEngine;
using UnityEngine.Events;
using UnityEngine.EventSystems;

public abstract class MyButton : MonoBehaviour, IPointerDownHandler, IPointerUpHandler, IPointerEnterHandler, IPointerExitHandler, IPointerClickHandler
{
    public UnityAction OnClickUnityAction;

    [Header("SE Sound")] [SerializeField] [ Tooltip("SE when mouse enter")]
    private SeSoundData.SE _enterSe;

    [SerializeField] [ Tooltip("SE when clicked")]
    private SeSoundData.SE _clickedSe;


    protected abstract void OnClicked();
    protected abstract void OnMouseDown();
    protected abstract void OnMouseUp();
    protected abstract void OnMouseEnter();
    protected abstract void OnMouseExit();

    public void OnPointerDown(PointerEventData  eventData) { OnMouseDown(); }
    public void OnPointerUp(PointerEventData    eventData) { OnMouseUp(); }
    public void OnPointerEnter(PointerEventData eventData) { OnMouseEnter(); }
    public void OnPointerExit(PointerEventData  eventData)
    {
        if (_enterSe != SeSoundData.SE.None) { SoundManager.Instance.PlaySe(_enterSe); }

        OnMouseExit();
    }
    public void OnPointerClick(PointerEventData eventData)
    {
        if (_clickedSe != SeSoundData.SE.None) { SoundManager.Instance.PlaySe(_clickedSe); }

        OnClickUnityAction?.Invoke();
        OnClicked();
    }
}