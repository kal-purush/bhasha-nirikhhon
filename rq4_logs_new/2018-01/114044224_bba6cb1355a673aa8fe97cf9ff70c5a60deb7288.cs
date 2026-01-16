using UnityEngine;
using System.Collections;
using System.Collections.Generic;

[ExecuteInEditMode]
public class ScreenSpaceRefractions : MonoBehaviour
{
    [HideInInspector]
    [SerializeField]
    private Camera _camera;
    private int _downResFactor = 1;

    [SerializeField]
    [Range(0, 1)]
    [HideInInspector]
    private float _refractionVisibility = 0;
    [SerializeField]
    [Range(0, 0.1f)]
    [HideInInspector]
    private float _refractionMagnitude = 0;

    [SerializeField]private Color32 changeColor;

    private string _globalTextureName = "_GlobalRefractionTex";
    private string _globalVisibilityName = "_GlobalVisibility";
    private string _globalMagnitudeName = "_GlobalRefractionMag";


    void OnEnable()
    {
        GenerateRT();
    }



    void GenerateRT()
    {
        _camera = GetComponent<Camera>();

        if (_camera.targetTexture != null) {
            RenderTexture temp = _camera.targetTexture;

            _camera.targetTexture = null;
            DestroyImmediate(temp);
        }

        _camera.targetTexture = new RenderTexture(_camera.pixelWidth << _downResFactor, _camera.pixelHeight << _downResFactor, 16);
        _camera.targetTexture.filterMode = FilterMode.Point;


        Shader.SetGlobalTexture(_globalTextureName, _camera.targetTexture);
    }
}