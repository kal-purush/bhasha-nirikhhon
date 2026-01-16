using System.Collections;
using System.Collections.Generic;
using UnityEngine;

//Singleton class to store item sprite assets
//When a new item is created, add a public variable to store its sprite in the class below
//The switch statement in Item.cs should then be updated to point towards that new item's sprite in this class.
//Author: Ishaiah Cross

public class ItemAssets : MonoBehaviour
{
    public static ItemAssets Instance {get; private set; }

    void Awake() {
        Instance = this;
    }

    public Sprite HeartSprite;
    public Sprite StarSprite;
}