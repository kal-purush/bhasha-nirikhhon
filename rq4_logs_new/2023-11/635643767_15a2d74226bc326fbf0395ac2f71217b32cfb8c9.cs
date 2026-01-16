using System.Collections;
using UnityEngine;

public class CameraShake : SingletonMonobehaviour<CameraShake>
{
    private bool trigger = default;
    private float duration = 1.5f;

    //===========================================================================
    void Update()
    {
        if (trigger)
        {
            StartCoroutine(Shake());
            trigger = false;
        }
    }

    //===========================================================================
    private IEnumerator Shake()
    {
        Vector3 startPosition = transform.position;
        float elapsedTime = 0.0f;

        while (elapsedTime < duration)
        {
            elapsedTime += Time.deltaTime;

            Vector3 movePos = new Vector3(Player.Instance.transform.position.x, Player.Instance.transform.position.y, -10.0f);
            transform.position = movePos + Random.insideUnitSphere * 0.25f;

            yield return null;
        }

        transform.position = startPosition;
    }

    //===========================================================================
    public void SetCameraShakeOn()
    {
        trigger = true;
    }
}