using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class WorldItem : Collectable
{
    //Spawns an item in the world at the provided position and with the given item type
    public static WorldItem Spawn(Vector3 position, Item itemType) {
        Transform tf = Instantiate(ItemAssets.Instance.pfWorldItem, position, Quaternion.identity);
        WorldItem item = tf.GetComponent<WorldItem>();
        
        SpriteRenderer renderer = tf.GetComponent<SpriteRenderer>();
        renderer.sprite = itemType.GetSprite();
        renderer.sortingOrder = 1;

        item.SetItem(itemType);

        return item;
    }

    public void SetItem(Item item) {
        this.item = item;
    }

    public void DestroySelf() {
        Destroy(this.gameObject);
    }

}