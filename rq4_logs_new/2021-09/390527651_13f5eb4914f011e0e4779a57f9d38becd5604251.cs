using System.Collections;
using System.Collections.Generic;
using UnityEngine;

/*
    This class acts as a container for several groups of sprites (stored in List<Sprite> types) that can be set in the editor.
    This can serve as an easy way to access sprites at runtime.
*/
public class SpriteListDictionary : MonoBehaviour
{
    [System.Serializable]
    public struct SpriteKeyListPair {
        public string key;
        public Sprite[] spriteCollection;
    }

    public SpriteKeyListPair[] spriteGroups;

    /*
    Gets a list of sprites from the dictionary using the provided key
    @param key The string index of the sprite to add
    @return The list of sprites associated with this key, or null if no value existed with that key
    */
    public Sprite[] GetSpriteList(string key) {
        foreach(SpriteKeyListPair pair in spriteGroups) {
            if(pair.key == key) {
                return pair.spriteCollection;
            }
        }

        return null;
    }
}