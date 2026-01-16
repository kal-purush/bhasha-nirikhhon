using System;
using UnityEngine;
using UnityEngine.Events;
using UnityEngine.EventSystems;
using UnityEngine.Serialization;
using UnityEngine.UI;

[RequireComponent(typeof(Image))]
public abstract class BaseButton : MonoBehaviour, IPointerDownHandler, IPointerUpHandler, IPointerEnterHandler, IPointerExitHandler, IPointerClickHandler
{
     public UnityEvent _onClickUnityAction;

    private Image   _buttonImage;

    private                  Sprite _normalsSprite;
    [SerializeField] private Sprite _pressedSprite;
    [SerializeField] private Sprite _hoverSprite;

    [SerializeField] private Animation _pressedAnimation;


    [SerializeField] private AudioClip _enterSe;

    [SerializeField] [ Tooltip("SE when clicked")]
    private AudioClip _clickedSe;


    public void  Start() { GetComponentSetup(); }

    private void GetComponentSetup()
    {
        _buttonImage   = GetComponent<Image>();
        _normalsSprite = _buttonImage.sprite;
    }


    protected abstract void OnClicked();
    protected abstract void OnMouseDown();
    protected abstract void OnMouseUp();
    protected abstract void OnMouseEnter();
    protected abstract void OnMouseExit();

    public void OnPointerDown(PointerEventData eventData)
    {
        if (_pressedSprite != null) { _buttonImage.sprite = _pressedSprite; } //   Change texture to pressed state

        if (_clickedSe     != null) { SoundManager.Instance.PlaySe(_clickedSe); }

        OnMouseDown();
    }
    public void OnPointerUp(PointerEventData   eventData) { OnMouseUp(); }

    public void OnPointerEnter(PointerEventData eventData)
    {
        if (_enterSe       != null) { SoundManager.Instance.PlaySe(_enterSe); }

        if (_normalsSprite != null) { _buttonImage.sprite = _hoverSprite; }  //   Reset texture to normal state

        OnMouseEnter();
    }
    public void OnPointerExit(PointerEventData  eventData)
    {
        if (_normalsSprite != null) { _buttonImage.sprite = _normalsSprite; } //   Change texture to hover state

        OnMouseExit();
    }
    public void OnPointerClick(PointerEventData eventData)
    {
        if (_pressedAnimation != null) { _pressedAnimation.Play(); }


        _onClickUnityAction?.Invoke();
        OnClicked();
    }

    public void ReverseAnimation()
    {
        if (_pressedAnimation != null)
        {
            _pressedAnimation["AnimRoll"].speed = -1;

            _pressedAnimation.Play();
        }
    }
}