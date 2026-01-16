using System.Collections;
using System.Collections.Generic;
using UnityEngine;

/*
This class spawns a collectable at its location, then destroys itself
This is useful if you want to add a collectable to a scene without writing a script to spawn it in

To use this class, create an empty game object and add this script to it as a component. Then, in the inspector,
select which item you would like it to spawn and enter the stack amount.
*/

public class ItemSpawner : MonoBehaviour
{
    public Item item; //The item that this spawner will spawn

    void Start()
    {
        //Spawn the item
        //Set the z-coordinate to 0 so the item doesn't appear behind the background
        Collectable.Spawn(new Vector3(transform.position.x, transform.position.y, 0), item);
        Destroy(gameObject);
    }
}