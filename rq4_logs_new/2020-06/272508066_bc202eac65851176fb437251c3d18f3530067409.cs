using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class BillboardHealth : MonoBehaviour
{
    private Camera _camera;
    private Canvas _canvas;
    private bool Enabled = true;

    // Start is called before the first frame update
    void Start()
    {
        _canvas = GetComponent<Canvas>();

        if (_canvas)
        {
            _camera = _canvas.worldCamera;
        }

        if (!_canvas || !_camera)
        {
            Enabled = false;
        }
    }

    // Update is called once per frame
    void Update()
    {
        if (Enabled)
        {
            transform.LookAt(_camera.transform);
        }
    }
}