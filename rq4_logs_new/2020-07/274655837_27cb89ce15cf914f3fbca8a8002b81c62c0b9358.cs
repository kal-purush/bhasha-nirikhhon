using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Window : MonoBehaviour
{
    public GameObject game;

    void Start()
    {
        ScreenOptimization.setColider(gameObject, this.GetComponent<BoxCollider2D>());
    }

    private void OnTriggerExit2D(Collider2D collision)
    {
        Checker check = collision.gameObject.GetComponent<Checker>();
        if (collision.gameObject.transform.position.y > 0 && check.field == Checker.Field.Down)
        {
            check.field = Checker.Field.Up;
            game.GetComponent<Mode>().changeCount(collision.gameObject);
        }
        if (collision.gameObject.transform.position.y < 0 && check.field == Checker.Field.Up)
        {
            check.field = Checker.Field.Down;
            game.GetComponent<Mode>().changeCount(collision.gameObject);
        }
    }
}