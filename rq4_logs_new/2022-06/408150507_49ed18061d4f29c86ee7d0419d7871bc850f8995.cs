using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Render_Texture : MonoBehaviour
{
    public RenderTexture rt;
    public RenderTextureFormat rtf;
    public int width;
    public int height;
    public FilterMode filtermode;
    public TextureWrapMode WrapMode;

    void Start()
    {
        rt = new RenderTexture(width, height, 16, rtf);
        rt.filterMode = filtermode;
        rt.wrapMode = WrapMode;
        rt.Create();
    }
}