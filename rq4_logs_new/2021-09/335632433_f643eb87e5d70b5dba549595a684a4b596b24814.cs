using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.InputSystem;

public class MotiEffect : MonoBehaviour
{
    [SerializeField] GameObject MotiTrailEffect;
    // Start is called before the first frame update
    void Start()
    {
        MotiTrailEffect.SetActive(false);
    }

    // Update is called once per frame
    void Update()
    {
        
    }
    void OnFire(InputValue input)
    {
        var pressed = input.isPressed;
        MotiTrailEffect.SetActive(true);

        if (!pressed)
        {
            MotiTrailEffect.SetActive(false);
        }
    }
    public void EffectActive()
    {
        MotiTrailEffect.SetActive(true);
    }
    public void EffectDestroy()
    {
        MotiTrailEffect.SetActive(false);

    }
}