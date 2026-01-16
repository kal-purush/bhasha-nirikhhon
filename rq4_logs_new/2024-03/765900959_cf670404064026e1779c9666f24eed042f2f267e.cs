using UnityEngine;
using UnityEngine.Events;
using UnityEngine.InputSystem;
using static PlayerInputActions;

namespace Platformer
{
    [CreateAssetMenu(fileName = "InputReader", menuName = "OneBullet/Inputs/InputReader")]
    public class InputReader : ScriptableObject, IPlayerActions
    {
        public event UnityAction<Vector2> Move = delegate { };
        public event UnityAction<Vector2, bool> Look = delegate { };    // The Bool refers to if the player is using a Mouse (true) or a controller (false)
        public event UnityAction Jump = delegate { };
        public event UnityAction Fire = delegate { };
        public event UnityAction Run = delegate { };

        public event UnityAction EnableAimingMode = delegate { };
        public event UnityAction DisableAimingMode = delegate { };

        PlayerInputActions inputActions;

        public Vector3 Direction => inputActions.Player.Move.ReadValue<Vector2>();

        void OnEnable()
        {
            if (inputActions == null)
            {
                inputActions = new PlayerInputActions();
                inputActions.Player.SetCallbacks(this);
            }
            inputActions.Enable();
        }

        public void OnMove(InputAction.CallbackContext context)
        {
            Move.Invoke(context.ReadValue<Vector2>());
        }

        public void OnLook(InputAction.CallbackContext context)
        {
            Look.Invoke(context.ReadValue<Vector2>(), IsDeviceMouse(context));
        }

        public void OnFire(InputAction.CallbackContext context)
        {
            Fire.Invoke();
        }

        public void OnJump(InputAction.CallbackContext context)
        {
            Jump.Invoke();
        }

        public void OnRun(InputAction.CallbackContext context)
        {
            // FIXME : On va mettre du run ou pas ? Jsp mais pour l'instant on le laisse
            Run.Invoke();
        }

        public void OnEnableAimingMode(InputAction.CallbackContext context)
        {
            switch (context.phase)
            {
                case InputActionPhase.Started:
                    EnableAimingMode.Invoke();
                    break;
                case InputActionPhase.Canceled:
                    DisableAimingMode.Invoke();
                    break;
            }
        }

        private bool IsDeviceMouse(InputAction.CallbackContext context) => context.control.device.name == "Mouse";
    }
}