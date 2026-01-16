using System.Collections;
using System.Collections.Generic;
using Scenes.CameraTesting.Camara;
using UnityEngine;

/// <summary>
/// Adjusts the camera target's horizontal position (bias) based on player input direction.
/// </summary>
public class CameraBias : MonoBehaviour
{
    [SerializeField]
    private GameObject _cameraTarget;

    [SerializeField]
    private PlayerInput _playerInput;

    [SerializeField]
    private float expectedBias = 1f;

    [SerializeField]
    private float lerpSpeed = 5f;

    private float _desiredX = 0f;

    /// <summary>
    /// Updates the camera target's horizontal position every fixed frame, creating a directional bias.
    /// </summary>
    private void FixedUpdate()
    {
        float inputX = _playerInput.Movement.x;

        if (inputX > 0.1f)
        {
            _desiredX = expectedBias;
        }
        else if (inputX < -0.1f)
        {
            _desiredX = -expectedBias;
        }

        Vector3 localTargetPos = _cameraTarget.transform.localPosition;
        localTargetPos.x = Mathf.Lerp(localTargetPos.x, _desiredX, Time.deltaTime * lerpSpeed);
        _cameraTarget.transform.localPosition = localTargetPos;
    }
}