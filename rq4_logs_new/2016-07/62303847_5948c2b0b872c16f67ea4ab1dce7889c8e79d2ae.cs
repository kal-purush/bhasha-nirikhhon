using UnityEngine;
using System.Collections;
using UnityEngine.UI;

public class SyncSpriteImage : MonoBehaviour
{
    private Image image = null;
    private SpriteRenderer sprite = null;

	// Use this for initialization
	void Start ()
    {
        image = GetComponent<Image>();
        sprite = GetComponent<SpriteRenderer>();

        if(image == null || sprite == null)
        {
            Debug.LogWarning("image or sprite is NULL");
            enabled = false;
        }
	}
	
	// Update is called once per frame
	void Update ()
    {
        image.sprite = sprite.sprite;
	}
}