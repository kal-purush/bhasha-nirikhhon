using System.Collections.Generic;
using System.Linq;
using UnityEngine;

public class PlayerThingsCollector : MonoBehaviour
{
    private PlayerHandController _playerHandController;

    private void Start()
    {
        _playerHandController = GetComponent<PlayerHandController>();
    }
    
    private void OnTriggerEnter2D(Collider2D col)
    {
        ThingTag thing = col.gameObject.GetComponent<ThingTag>();

        if (thing == null)
        {
            return;
        }

        if (PlayerInventory.ThingsAmount() == 0 
            && _playerHandController.HandThingId == -1)
        {
            _playerHandController.HandThingId = thing.Pickup();
            return;
        }

        PlayerInventory.Put(thing.Pickup());
    }
}