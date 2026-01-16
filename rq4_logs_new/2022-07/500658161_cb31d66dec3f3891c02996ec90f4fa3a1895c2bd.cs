using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ColliderDestroyWithAnimate : MonoBehaviour
{
    [SerializeField]
    private Sprite[] listSprite;
    [SerializeField]
    private NetworkIdentity networkIdentity;
    
    public void Update()
    {
        if (listSprite.Length == 0) return;
        float health = networkIdentity.getHealthBar().slider.value;
        float maxHealth = networkIdentity.getHealthBar().slider.maxValue;
        if (health == maxHealth) return;
        float rate = Mathf.Round((health / maxHealth) * 100);
        if (70 <= rate && rate < 100)
        {
            transform.gameObject.GetComponent<SpriteRenderer>().sprite = listSprite[0];
        }
        else
        {
            if (30 <= rate && rate < 70)
                transform.gameObject.GetComponent<SpriteRenderer>().sprite = listSprite[1];
            else
                transform.gameObject.GetComponent<SpriteRenderer>().sprite = listSprite[2];
        }
    }
}