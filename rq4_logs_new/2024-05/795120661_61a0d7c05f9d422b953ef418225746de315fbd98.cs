#nullable enable
using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class HpGaugeMono : MonoBehaviour
{
    [SerializeField] SpriteRenderer hpGaugeSpriteRenderer = null!;
    Material? _hpGaugeMaterial;
    
    void Start()
    {
        _hpGaugeMaterial = hpGaugeSpriteRenderer.material;
    }
    
    public void SetRatio(float ratio)
    {
        if (_hpGaugeMaterial == null)
        {
            Debug.LogWarning("_hpGaugeMaterial is null");
            return;
        }
        _hpGaugeMaterial.SetFloat("_Ratio", ratio);
    }
}