using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class BackdropSlider : MonoBehaviour
{

    private Renderer renderer;

    // Start is called before the first frame update
    void Start()
    {
        renderer = GetComponent<Renderer>();
    }

    // Update is called once per frame
    void Update()
    {
        if (Input.GetKey(KeyCode.D))
        {
            scrollBackDrop(0.5f);
        }
    }
    
    /*
     * Speed should be the distance between the handlebar's
     * left hand's position last frame and its position this frame
     */
    public void scrollBackDrop(float speed)
    {
        float tempx = renderer.material.mainTextureOffset.x;
        renderer.material.mainTextureOffset = new Vector2(tempx += speed * Time.deltaTime, 0);
    }
}