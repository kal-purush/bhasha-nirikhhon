using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using TMPro;
using UnityEngine.UI;

public class MuliLetter : MonoBehaviour
{
    public TextMeshProUGUI text;
    public Image image;

    public MuliTimer resolveTimer = new MuliTimer(5f);
    public MuliTimer switchTimer = new MuliTimer(0.1f);

    public bool settled = false;

    public Sprite[] sprites;


    // Start is called before the first frame update
    void Start()
    {
        sprites = Resources.LoadAll<Sprite>("Hiero");
        text.enabled = false;
        image.sprite = sprites[UnityEngine.Random.Range(0, sprites.Length)];
    }

    // Update is called once per frame
    void Update()
    {
        if (!settled)
        {
            if (switchTimer.isDone())
            {
                switchTimer.reset();
                //change sprite
                image.sprite = sprites[UnityEngine.Random.Range(0,sprites.Length)];
            }
            else
            {
                switchTimer.update(Time.deltaTime);
            }
            if (resolveTimer.isDone())
            {
                settled= true;
                image.enabled= false;
                text.enabled = true;
            }
            else
            {
                resolveTimer.update(Time.deltaTime);
            }
        }  
    }
}