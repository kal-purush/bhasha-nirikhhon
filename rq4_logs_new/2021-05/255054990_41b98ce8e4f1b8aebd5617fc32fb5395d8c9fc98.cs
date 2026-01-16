using DG.Tweening;
using UnityEngine;

public class PhoneNotificationService : MonoBehaviour {
    [Header("Settings")]
    [SerializeField]
    private GameObject successNotificationObj;

    private GameObject currentNotification;

    private CanvasGroup canvasGroup;

    public static PhoneNotificationService Instance;

    private void Awake() {
        if (Instance != null && Instance != this) {
            Destroy(this);
        } else {
            Instance = this;
            this.canvasGroup = GetComponent<CanvasGroup>();
            this.successNotificationObj.SetActive(false);
            this.canvasGroup.alpha = 0;
            this.canvasGroup.interactable = false;
            this.canvasGroup.blocksRaycasts = false;
        }
    }

    public void DisplayBuySuccessNotification() {
        this.successNotificationObj.SetActive(true);
        this.currentNotification = successNotificationObj;
        this.canvasGroup.DOComplete();
        this.canvasGroup.DOFade(1, .3f);
        this.canvasGroup.interactable = true;
        this.canvasGroup.blocksRaycasts = true;
    }

    public void RemoveNotification() {
        this.canvasGroup.DOComplete();
        this.canvasGroup.DOFade(0, .3f).OnComplete(() => {
            this.currentNotification.SetActive(false);
            this.currentNotification = null;
            this.canvasGroup.interactable = false;
            this.canvasGroup.blocksRaycasts = false;
        });
    }
}