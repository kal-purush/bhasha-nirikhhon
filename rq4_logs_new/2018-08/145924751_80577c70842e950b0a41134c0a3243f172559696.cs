using UnityEngine;
using UnityEngine.EventSystems;

public class SpriteDetector : MonoBehaviour, IPointerDownHandler, IPointerEnterHandler {

    Physics2DRaycaster physicsRaycaster;
    public float distance = 100f;

    private void Start() {
        physicsRaycaster = GameObject.FindObjectOfType<Physics2DRaycaster>();
        if (physicsRaycaster == null) {
            Camera.main.gameObject.AddComponent<Physics2DRaycaster>();
        }
    }

    public void OnPointerDown(PointerEventData eventData) {
        Debug.Log("Clicked: " + eventData.pointerCurrentRaycast.gameObject.name);
    }

    public void OnPointerEnter(PointerEventData eventData) {
        Debug.Log("Mouse Enter: " + eventData.pointerCurrentRaycast.gameObject.name);
    }
}