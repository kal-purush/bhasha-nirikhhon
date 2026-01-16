using UnityEngine;
using Cinemachine;

public class CineMachinePOV : CinemachineExtension
{
    [SerializeField] private float horizontalVelocidad = 10f;
    [SerializeField] private float verticalVelocidad = 10f;
    [SerializeField] private float anguloPermitido = 80f;
    private InputManager inputManager;
    private Vector3 rotacion;
    protected override void Awake()
    {
        inputManager = InputManager.Instance;
        base.Awake();
    }
    protected override void PostPipelineStageCallback(CinemachineVirtualCameraBase vcam, CinemachineCore.Stage stage, ref CameraState state, float deltaTime)
    {
        if (vcam.Follow)
        {
            if(stage == CinemachineCore.Stage.Aim)
            {
                if (rotacion == null) rotacion = transform.localRotation.eulerAngles;
                Vector2 deltaInput = inputManager.jugadorGetRaton();
                rotacion.x += deltaInput.x * verticalVelocidad * Time.deltaTime;
                rotacion.y += deltaInput.y * horizontalVelocidad * Time.deltaTime;

                rotacion.y = Mathf.Clamp(rotacion.y, -anguloPermitido, anguloPermitido);
                state.RawOrientation = Quaternion.Euler(-rotacion.y, rotacion.x, 0f);
            }
        }
    }
}