using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Player_Rotation_Fun : MonoBehaviour
{
    [SerializeField] private float RotationDuration;
    [SerializeField] private float RotationAngle;

    void OnTriggerEnter(Collider other)
    {
        if (other.CompareTag("Player"))
        {
            Rotation_Player(other.gameObject, RotationAngle);
        }
    }

    public void Rotation_Player(GameObject target, float rotation)
    {
        StartCoroutine(SmoothRotatePlayer(target, rotation));
    }

    IEnumerator SmoothRotatePlayer(GameObject target, float rotationAngle)
    {
        yield return new WaitForSeconds(2);

        Quaternion startRotation = target.transform.rotation;
        Quaternion targetRotation = startRotation * Quaternion.Euler(0, rotationAngle, 0); // 基於當前旋轉，增加相對旋轉
        float elapsedTime = 0f;
        while (elapsedTime < RotationDuration)
        {

            target.transform.rotation = Quaternion.Slerp(startRotation, targetRotation, elapsedTime / RotationDuration);
            elapsedTime += Time.deltaTime;
            yield return null; // Continue the rotation over multiple frames
        }

        target.transform.rotation = targetRotation; // 確保最後設置為目標旋轉
    }
}
