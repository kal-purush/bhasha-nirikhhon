using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ActivatorController : MonoBehaviour
{
    public ActivatorConfiguration config;

    private void OnTriggerEnter2D(Collider2D collision)
    {
        config.button.transform.position -= new Vector3(0, 0.15f, 0);
        config.isActivated = true;
    }

    private void OnTriggerStay2D(Collider2D collision)
    {
        config.isActivated = true;
    }

    private void OnTriggerExit2D(Collider2D collision)
    {

        config.button.transform.position += new Vector3(0, 0.15f, 0);
        config.isActivated = false;
    }
}