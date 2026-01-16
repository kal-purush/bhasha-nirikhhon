using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class Tooltip : MonoBehaviour
{
    public float padding = 4.0f;

    private UnityEngine.Camera uiCamera;
    private Text tooltipText;
    private RectTransform backgroundTf;

    void Awake() {
        tooltipText = transform.Find("Text").GetComponent<Text>();
        backgroundTf = transform.Find("Background").GetComponent<RectTransform>();

        GameObject inventory = transform.parent.gameObject;
        uiCamera = inventory.GetComponentInParent<UnityEngine.Canvas>().worldCamera;
    }

    public void SetText(string text) {
        tooltipText.text = text;
        //Calculate size of tooltip background based on the provided text
        Vector2 backgroundSize = new Vector2((tooltipText.preferredWidth + padding * 10) * 0.2f, (tooltipText.preferredHeight + padding * 5) * 0.2f);
        backgroundTf.sizeDelta = backgroundSize;
    }

    /**
    Shows or hides the tooltip from being visible
    */
    public void ShowTooltip(bool show) {
        transform.SetAsLastSibling();
        gameObject.SetActive(show);
    }

    private void Update() {
        //Convert the cursor position to a screen space coordinate that can be used to position the tooltip within the inventory frame
        Vector2 localPoint;
        RectTransformUtility.ScreenPointToLocalPointInRectangle(transform.parent.GetComponent<RectTransform>(), Input.mousePosition, uiCamera, out localPoint);
        transform.localPosition = localPoint;
    }
}