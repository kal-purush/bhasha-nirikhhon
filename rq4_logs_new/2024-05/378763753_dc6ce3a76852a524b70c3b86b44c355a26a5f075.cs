using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Chest : MonoBehaviour
{
    bool animateArrow = true;
    Transform arrow;
    SpriteRenderer arrowRenderer;
    byte animationTimer = 0;
    byte[] arrowAnimationHeights;
    Vector3 arrowRefPos;
    byte frameTime = 4;
    byte arrowAnimationIndex = 0;

    // Start is called before the first frame update
    void Start()
    {
        arrow = this.transform.Find("Arrow");
        arrowRenderer = arrow.GetComponent<SpriteRenderer>();

        arrowRefPos = arrow.transform.localPosition;

        arrowAnimationHeights = new byte[8] { 0, 1, 2, 4, 2, 1, 0, 0 };
    }

    // Update is called once per frame
    void Update()
    {
        
    }

    private void FixedUpdate()
    {
        if (animateArrow)
        {
            if (!arrowRenderer.enabled) arrowRenderer.enabled = true;
            if (animationTimer == frameTime)
            {
                arrowAnimationIndex++;
                arrow.transform.localPosition = new Vector3(arrowRefPos.x, arrowRefPos.y - ((arrowAnimationHeights[arrowAnimationIndex]/16f)), arrowRefPos.z);
                animationTimer = 0;

                if (arrowAnimationIndex == arrowAnimationHeights.Length - 1)
                    arrowAnimationIndex = 0;
            }
            else
                animationTimer++;

        }
        else if (arrowRenderer.enabled) arrowRenderer.enabled = false;
    }

    private void OnTriggerEnter2D(Collider2D other)
    {
        if (other.CompareTag("Player"))
        {
            Player p = GameObject.FindGameObjectWithTag("Player").GetComponent<Player>();
            if (p.isFacing(this.transform))
                Debug.Log("YES");
            else
                Debug.Log("NO");
        }

    }


}