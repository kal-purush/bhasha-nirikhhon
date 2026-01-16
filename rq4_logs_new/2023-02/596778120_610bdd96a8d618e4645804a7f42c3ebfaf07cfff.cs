using System.Collections;
using System.Collections.Generic;
using Unity.VisualScripting;
using UnityEngine;

public class VFXRoot : MonoBehaviour
{
    [SerializeField]
    private VFXType _vfxType;
    public VFXType VFXType { get { return _vfxType; } private set { } }
    [SerializeField]
    private ParticleSystem _particles;
    void Start()
    {
        if (_particles == null)
            _particles = GetComponentInChildren<ParticleSystem>();
    }
    private void OnEnable()
    {
        StartCoroutine(DisableVFX());
    }
    private IEnumerator DisableVFX()
    {
        yield return new WaitForSeconds(_particles.main.duration);
        gameObject.SetActive(false);
    }
}