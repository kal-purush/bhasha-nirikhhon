using UnityEngine;
using System.Collections;

public class InscriptionSwitch : MonoBehaviour {

    public Sprite keyboardImage;
    public Sprite mobileImage;
    SpriteRenderer spriteRender;

    float playerDistance;
    float triggerDistance = 2.5f;
    Transform player;

    bool fadeIn = false;
	// Use this for initialization
    void Awake()
    {
        spriteRender = this.GetComponent<SpriteRenderer>();
        player = GameObject.FindGameObjectWithTag("Player").GetComponent<Transform>();
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
        Color c = spriteRender.material.color;
        c.a = 0f;
        spriteRender.material.color = c;

    }
    void Update()
    {
        playerDistance = Vector3.Distance(player.position , transform.position);
        //Debug.Log(gameObject.name +" : " +playerDistance);
        
        if(playerDistance < triggerDistance)
        {
            if (fadeIn == false)
            {
                fadeIn = true;
                //activate inscription
                //StopAllCoroutines();
                StartCoroutine(FadeIn());
            }
        }
        else
        {
            if(fadeIn == true)
            {
                fadeIn = false;
                //deactivate inscription
                //StopAllCoroutines();
                StartCoroutine(FadeOut());
            }
           
        }
        
    }

    IEnumerator FadeOut()
    {
        //Debug.Log("fadeOut");
        for (float f = 1f; f >= 0; f -= 0.1f)
        {
            //yield return new WaitForSeconds(1);
            Color c = spriteRender.material.color;
            c.a = f;
            spriteRender.material.color = c;
            yield return null;
        }
    }

    IEnumerator FadeIn()
    {
        //Debug.Log("fadein");
        for (float f = 0; f <= 1f; f += 0.1f)
        {
            //yield return new WaitForSeconds(1);
            Color c = spriteRender.material.color;
            c.a = f;
            spriteRender.material.color = c;
            yield return null;
        }
    }



}