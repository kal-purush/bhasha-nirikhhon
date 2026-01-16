using DG.Tweening;
using UnityEngine;
using UnityEngine.EventSystems;

public class CardHighlightInteraction : MonoBehaviour, IPointerEnterHandler, IPointerExitHandler
{

	private RectTransform rectTransform;
	private Canvas canvas;

	public void Awake()
	{
		rectTransform = GetComponent<RectTransform>();
		canvas = GetComponent<Canvas>();
	}

	public void OnPointerEnter(PointerEventData eventData)
	{
		canvas.overrideSorting = true;
		rectTransform.DOScale(Vector3.one, 0.3f);
	}

	public void OnPointerExit(PointerEventData eventData)
	{
		canvas.overrideSorting = false;
		rectTransform.DOScale(Vector3.one * 0.75f, 0.3f);
	}
}