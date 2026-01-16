using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class EnemySniper : MonoBehaviour
{
    private Animator anim;
    private GameObject player;
    private Image img;

    public Vector2 screenPoint;

    private void Start()
    {
        anim = GetComponent<Animator>();
        player = GameObject.FindGameObjectWithTag("Player");
        img = GameObject.FindGameObjectWithTag("OverlayFX").GetComponentInChildren<Image>();


        img.enabled = false;
    }

    private void Update()
    {
        screenPoint = RectTransformUtility.WorldToScreenPoint(Camera.main, transform.position);

        if(player.transform.position.x - this.transform.position.x < 40 && player.transform.position.x - this.transform.position.x > 20)
        {
            img.rectTransform.position = screenPoint;
            img.enabled = true;
        }
        else
        {
            img.enabled = false;
        }
    }
}