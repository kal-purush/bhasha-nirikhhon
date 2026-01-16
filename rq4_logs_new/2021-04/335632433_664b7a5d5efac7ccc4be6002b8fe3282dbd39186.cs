using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class LockonCursor : MonoBehaviour
{
    // 自身のRectTransform
    protected RectTransform rectTransform;

    // カーソルのImage
    protected Image image;

    // ロックオン対象のTransform
    protected Transform LockonTarget { get; set; }

    void Start()
    {
        rectTransform = this.GetComponent<RectTransform>();

        image = this.GetComponent<Image>();
        image.enabled = false;
    }

    void Update()
    {
        if (image.enabled)
        {
            rectTransform.Rotate(0, 0, 1f);

            if (LockonTarget != null)
            {
                Vector3 targetPoint = Camera.main.WorldToScreenPoint(LockonTarget.position);
                rectTransform.position = targetPoint;
            }
        }
    }

    public void OnLockonStart(Transform target)
    {
        image.enabled = true;
        LockonTarget = target;
    }

    public void OnLockonEnd()
    {
        image.enabled = false;
        LockonTarget = null;
    }
}