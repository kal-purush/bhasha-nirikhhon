using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class ImageAlpha : MonoBehaviour
{
    [SerializeField] Image image;
    [SerializeField] float speed = 20f;
    Color color;
    public bool IsAlpha = false;

    // Start is called before the first frame update
    void Start()
    {
        image = GetComponent<Image>();
        color = image.color;
        color.a = 0;
    }

    // Update is called once per frame
    void Update()
    {
        if (!IsAlpha) return;

        color.a += speed * Time.deltaTime;
        color.a = Mathf.Clamp(color.a, 0, 255);
        image.color = color;
    }
}