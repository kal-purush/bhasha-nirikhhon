using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class GameImageScript : MonoBehaviour
{
    private Image _spriteRenderer;
    private float _opasity;
    private Color _color;
    private bool _opasityPlus = true;
    private bool _opasityMinus = false;

    private void Start()
    {
        _opasity = 0;
        _spriteRenderer = GetComponent<Image>();
        _color = _spriteRenderer.color;
        _color.a = _opasity;
    }

    private void Update()
    {
        if (_opasityPlus)
        {
            _spriteRenderer.color = _color;
            _color.a = _opasity;
            if (_opasity < 255)
            {
                _opasity += Time.deltaTime * 1.3f;
            }
            else
            {
                _opasity = 255;
                _opasityPlus = false;
            }
        }

        if (_opasityMinus)
        {
            _spriteRenderer.color = _color;
            _color.a = _opasity;
            if (_opasity > 0)
            {
                _opasity -= Time.deltaTime * 1.3f;
            }
            else
            {
                _opasity = 0;
                _opasityMinus = false;
            }
        }
    }

    public void GoOpacityMinus()
    {
        _opasityPlus = false;
        _opasityMinus = true;
    }

}