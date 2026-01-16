using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Food : MonoBehaviour
{
    public BoxCollider2D playArea;

    private GameManager gameManager;

    private void Start()
    {
        gameManager = GameObject.Find("GameManager").GetComponent<GameManager>();
    }

    private void OnTriggerEnter2D(Collider2D collision)
    {
        if (collision.tag == "Sandworm")
        {
            if (!collision.gameObject.GetComponent<Sandworm>().isBurrowed)
            {
                Reposition();
                gameManager.IncreaseScore(1);
            }
        }
    }

    private void Reposition()
    {
        Bounds playAreaBounds = this.playArea.bounds;

        float x = Random.Range(playAreaBounds.min.x, playAreaBounds.max.x);
        float y = Random.Range(playAreaBounds.min.y, playAreaBounds.max.y);

        this.transform.position = new Vector3(x, y, 0.0f);
    }
}