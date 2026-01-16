using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ChangeLighting : MonoBehaviour
{
    private Renderer _renderer;
    private Shader ambLight;
    private Shader regularLight;

    // Start is called before the first frame update
    void Start()
    {
        _renderer = GetComponent<Renderer>();
        regularLight = Shader.Find("Standard");
        ambLight = Shader.Find("Custom/ToonyLoons");
    }

    // Update is called once per frame
    void Update()
    {
        if (Input.GetKeyDown(KeyCode.Alpha1))
        {
            _renderer.material.shader = regularLight;
        }
        else if (Input.GetKeyDown(KeyCode.Alpha2))
        {
            _renderer.material.shader = ambLight;
        }
    }
}