using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class CameraShake : MonoBehaviour
{
    public bool shouldShake = false;
    public AnimationCurve shakeCurve;
    public float duration = .2f;

    [SerializeField] private float cameraShakeModifier;
    
    private void Update()
    {
        if (shouldShake)
        {
            shouldShake = false;
            StartCoroutine(ShakeCamera());
        }
    }

    private IEnumerator ShakeCamera()
    {
        Vector3 startPos = transform.position;
        float elapsedTime = 0f;

        while (elapsedTime < duration)
        {
            elapsedTime += Time.deltaTime;
            float strength = shakeCurve.Evaluate(elapsedTime / duration);
            transform.position = (startPos + Random.insideUnitSphere * strength) * cameraShakeModifier;
            yield return null;
        }

        transform.position = new Vector3(0, 0, -10);
    }

    [ContextMenu("ShakeCam")]
    public void StartShake()
    {
        shouldShake = true;
    }
    
}