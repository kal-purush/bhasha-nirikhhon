using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class EnemySniper : MonoBehaviour
{
    private Transform player;
    private Player playerVars;
    private Animator anim;
    private GameObject[] images;
    private Image img;

    private bool shoot;
    private float index;
    private float amplitudeX = 1f;
    private float amplitudeY = 1f;
    private float omegaX = 1f;
    private float omegaY = 1f;

    private static int activeReticles;

    public float counter;
    public Transform targetLockPos;

    private void Start()
    {
        player = GameObject.FindGameObjectWithTag("Player").GetComponent<Transform>();
        images = GameObject.FindGameObjectsWithTag("Reticle");
        if (!img)
        {
            for (int i = 0; i < images.Length; i++)
            {
                if (i == activeReticles)
                {
                    img = images[i].GetComponent<Image>();
                    activeReticles++;
                    break;
                }
                else
                    continue;
            }
        }
        playerVars = player.GetComponent<Player>();
        anim = GetComponent<Animator>();

        img.enabled = false;
    }

    private void Update()
    {

        if (Vector3.Distance(transform.position, player.transform.position) < 50)
        {
            counter += Time.deltaTime * 1.5f;

            transform.LookAt(player.transform.position, Vector3.up);

            if (counter >= 5)
            {
                anim.SetTrigger("Shoot");
                counter = 0;
            }
        }

        if (transform.position.x - player.transform.position.x < 40 && transform.position.x - player.transform.position.x > -20)
        {
            index += Time.deltaTime;

            float x = amplitudeX * Mathf.Sin(omegaX * index);
            float y = Mathf.Abs(amplitudeY * Mathf.Sin(omegaY * index));

            img.transform.localScale = new Vector3(x, y, 1);

            img.enabled = true;

            Vector2 screenPoint = RectTransformUtility.WorldToScreenPoint(Camera.main, targetLockPos.position);

            img.rectTransform.position = new Vector2(screenPoint.x, screenPoint.y);
        }
        else
        {
            img.enabled = false;
            activeReticles--;
        }
    }

    public void Shoot()
    {
        playerVars.SetHealth(playerVars.GetHealth() - 10);
    }
}