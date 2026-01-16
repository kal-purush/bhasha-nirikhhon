using UnityEngine;
using System.Collections;

public class InscriptionSwitch : MonoBehaviour {

    public Sprite keyboardImage;
    public Sprite mobileImage;
    SpriteRenderer spriteRender;
	// Use this for initialization
    void Awake()
    {
        spriteRender = this.GetComponent<SpriteRenderer>();
    }
	void Start ()
    {
        if(Application.isMobilePlatform)
        {
            spriteRender.sprite = mobileImage;
        }
        else
        {
            spriteRender.sprite = keyboardImage;
        }
	
	}
	
	
}