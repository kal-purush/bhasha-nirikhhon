using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ReplaceNormalsBuffer : MonoBehaviour
{
    [SerializeField] private Shader normalsShader;

    private RenderTexture renderTexture;
    private new Camera cam;

    private void Start()
    {
        Camera thisCam = GetComponent<Camera>();
        
        renderTexture = new RenderTexture(thisCam.pixelWidth, thisCam.pixelHeight, 24);
        
        Shader.SetGlobalTexture("_CamersNormalsTexture", renderTexture);
        
        
        GameObject copy = new GameObject("NormalsCamera");
        cam = copy.AddComponent<Camera>();
        cam.CopyFrom(thisCam);
        cam.transform.SetParent(transform);
        cam.targetTexture = renderTexture;
        cam.SetReplacementShader(normalsShader, "RenderType");
        cam.depth = thisCam.depth - 1;
    }
}