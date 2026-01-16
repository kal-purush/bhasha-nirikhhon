using DG.Tweening;
using Sim;
using UnityEngine;
using UnityEngine.EventSystems;
using UnityEngine.UI;

public class PhoneApplication : MonoBehaviour, IPointerClickHandler, IPointerExitHandler, IPointerEnterHandler {
    [SerializeField]
    private AudioClip hoverSound;

    [SerializeField]
    private AudioClip clickSound;

    [SerializeField]
    private GameObject application;

    [SerializeField]
    private Image icon;
    
    public GameObject Application => application;
    
    public void OnPointerClick(PointerEventData eventData) {
        HUDManager.Instance.PlaySound(clickSound, 1f);
        this.application.SetActive(true);
    }

    public void OnPointerEnter(PointerEventData eventData) {
        this.icon.transform.DOComplete();
        this.icon.transform.DOScale(new Vector3(1.1f, 1.1f, 1f), .3f);
        HUDManager.Instance.PlaySound(hoverSound, 1f);
    }

    public void OnPointerExit(PointerEventData eventData) {
        this.icon.transform.DOComplete();
        this.icon.transform.DOScale(new Vector3(1f, 1f, 1f), .3f);
    }
}