using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Antidote : MonoBehaviour
{
    private Collider2D antidoteCollider;

    // Use this delegate
    // for calling methods on obtaining antidote
    public delegate void AntidoteObtained();
    public AntidoteObtained OnObtainingAntidote;

    private void OnTriggerEnter2D(Collider2D collision)
    {
        var collider = collision.GetComponent<Player.General.PlayerController>();
        if (collider)
        {
            OnObtainingAntidote?.Invoke();
        }
    }

}