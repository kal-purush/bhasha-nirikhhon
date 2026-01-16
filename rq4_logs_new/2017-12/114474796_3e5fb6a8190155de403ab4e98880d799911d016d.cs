using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ResourceManager : MonoBehaviour {

    // Items
    public Sprite[] itemTextures = new Sprite[(int)Item.ITEM_COUNT];

    public static ResourceManager singleton;

	// Use this for initialization
	void Awake () {
        singleton = this;
	}

    public Sprite GetItemSprite(Item item)
    {
        Sprite itemSprite = null;

        itemSprite = itemTextures[(int)item];

        // Return the sprite for the item and if it is null, return the sprite for stone
        return itemSprite == null ? itemTextures[0] : itemSprite;
    }
}