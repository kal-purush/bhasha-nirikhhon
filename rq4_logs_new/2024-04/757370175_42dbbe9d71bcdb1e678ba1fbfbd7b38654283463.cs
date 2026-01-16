using UnityEngine;
using UnityEngine.UI;

public class ImageAnimation : MonoBehaviour
{
    public Sprite[] sprites;
    public int spritePerFrame = 6;
    public bool loop = true;
    public bool destroyOnEnd = false;

    private int index = 0;
    private Image image;
    private int frame = 0;

    private bool isPlaying = false;

    protected void Awake()
    {
        image = GetComponent<Image>();
    }

    protected void OnEnable()
    {
        isPlaying = true;
    }

    protected void OnDisable()
    {
        isPlaying = false;
    }

    protected void Update()
    {
        if (isPlaying)
        {
            if (!loop && index == sprites.Length) return;
            frame++;
            if (frame < spritePerFrame) return;
            image.sprite = sprites[index];
            frame = 0;
            index++;
            if (index >= sprites.Length)
            {
                if (loop) index = 0;
                if (destroyOnEnd) Destroy(gameObject);
            }
        }
    }
}