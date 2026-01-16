using UnityEngine;
using UnityEngine.EventSystems;

public class TurretCreator : MonoBehaviour, IBeginDragHandler, IDragHandler, IEndDragHandler {
    //TODO: fix these names
    private GameObject turretHover;
    private SpriteRenderer hoverRenderer;

    public Sprite turretSprite;
    public int turretMode;

    public void Awake() {
        turretHover = new GameObject();
        turretHover.AddComponent<SpriteRenderer>();
        turretHover.transform.SetParent(LevelManager.Instance.transform);
        turretHover.SetActive(false);
        hoverRenderer = turretHover.GetComponent<SpriteRenderer>();
        hoverRenderer.sprite = turretSprite;
    }

    public void OnBeginDrag(PointerEventData eventData) {
        GameManager.Instance.placeTurret = true;
        turretHover.SetActive(true);
    }

    public void OnDrag(PointerEventData eventData) {
        Vector3 mPos = Camera.main.ScreenToWorldPoint(Input.mousePosition);
        Point p = new Point((int)(mPos.x + 0.5f), (int)(mPos.y + 0.5f));
        if (LevelManager.Instance.IsTileValid(p)) hoverRenderer.color = new Color(1.0f, 1.0f, 1.0f, 0.5f);
        else hoverRenderer.color = new Color(1.0f, 0.0f, 0.0f, 0.5f);

        turretHover.transform.position = new Vector3(p.x, p.y, 0.0f);
    }

    public void OnEndDrag(PointerEventData eventData) {
        Vector3 mPos = Camera.main.ScreenToWorldPoint(Input.mousePosition);
        Point p = new Point((int)(mPos.x + 0.5f), (int)(mPos.y + 0.5f));
        turretHover.SetActive(false);
        LevelManager.Instance.PlaceTurret(p, turretMode);
        GameManager.Instance.placeTurret = false;
    }
}