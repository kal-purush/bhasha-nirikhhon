using Cinemachine;
using UnityEngine;

public class CinemachinePOVExtension : CinemachineExtension
{
    [SerializeField] private float horizontalSpeed = 10.0f;
    [SerializeField] private float verticalSpeed = 10.0f;
    
    [SerializeField] private float clampAngle = 90.0f;
    [SerializeField] private InputManager inputManager;
    private Vector3 startingRotation;

    protected override void Awake()
    {
        startingRotation = transform.localRotation.eulerAngles;
        base.Awake();
    }
    
    protected override void PostPipelineStageCallback(CinemachineVirtualCameraBase vcam, CinemachineCore.Stage stage, ref CameraState state, float deltaTime)
    {
        if (!Application.isPlaying || !vcam.Follow || stage != CinemachineCore.Stage.Aim) return;
        
        Vector2 deltaInput = inputManager.PlayerMouseDelta;
                    
        startingRotation.x += deltaInput.x * verticalSpeed * Time.deltaTime;
        startingRotation.y += deltaInput.y * horizontalSpeed * Time.deltaTime;
        startingRotation.y = Mathf.Clamp(startingRotation.y, -clampAngle, clampAngle);

        state.RawOrientation = Quaternion.Euler(-startingRotation.y, startingRotation.x, 0f);

    }
}