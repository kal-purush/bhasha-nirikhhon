using UnityEngine;
using UnityEngine.Serialization;

public class BackgroundScroll : MonoBehaviour
{
    public float speed;
    [SerializeField] private Renderer _renderer;
    [FormerlySerializedAs("isScrolling")] public bool backgroundIsScrolling = true;

    void Update()
    {
        if (backgroundIsScrolling)
        {
            ScrollScreen();
        }
    }

    public void ScrollScreen()
    {
        _renderer.material.mainTextureOffset += new Vector2(speed * Time.deltaTime, 0);
    }
}