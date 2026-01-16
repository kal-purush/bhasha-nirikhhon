using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Magnet : MonoBehaviour
{
    public static float border;
    public static float force;
    Checker checker;

    private void Start()
    {
        checker = gameObject.GetComponent<Checker>();
    }

    // Update is called once per frame
    void FixedUpdate()
    {
        if (Game.activeGame && !checker.getMouseDown() && transform.position.x != border)
            transform.position = Vector2.MoveTowards(transform.position, new Vector2(border, transform.position.y), force * Time.fixedDeltaTime);
    }

}