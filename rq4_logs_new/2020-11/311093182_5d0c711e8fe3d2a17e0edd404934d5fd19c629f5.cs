using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class PropIndicatorAnimations : MonoBehaviour
{
    public float bobSpeed;
    public float bobHeight;
    public float lifeSpan;

    RectTransform rectTrans;
    CanvasRenderer canvasRenderer;

    float heightTimer;
    bool bobbingUp;

    float lifeTimer;

    void Start()
    {
        rectTrans = GetComponent<RectTransform>();
        canvasRenderer = GetComponent<CanvasRenderer>();
        canvasRenderer.SetAlpha(0);
        heightTimer = 0;
        bobbingUp = true;
        lifeTimer = 0;
    }
    
    void Update()
    {
        BobIndicator();
        FadeIndicator();
    }

    void BobIndicator()
    {
        if (heightTimer >= bobSpeed * 0.5) bobbingUp = false;
        else if (heightTimer <= bobSpeed * -0.5) bobbingUp = true;

        heightTimer += bobbingUp ? Time.deltaTime : -Time.deltaTime;

        Vector2 newPos = new Vector2(rectTrans.anchoredPosition.x,
                                     rectTrans.anchoredPosition.y + heightTimer * bobHeight);
        rectTrans.anchoredPosition = newPos;
    }

    void FadeIndicator()
    {
        lifeTimer += Time.deltaTime;

        if (lifeTimer > lifeSpan) lifeTimer = lifeSpan;

        float perc = lifeTimer / lifeSpan;
        float moddedPerc = perc;
        if (perc >= 0.5f)
        {
            moddedPerc = 0.5f + (0.5f - perc);
        }

        canvasRenderer.SetAlpha(moddedPerc * 2);

        if (lifeTimer == lifeSpan)
        {
            Destroy(gameObject);
        }
    }
}