using System;
using System.Collections;
using InputSystem;
using UnityEngine.InputSystem;
using Object = UnityEngine.Object;

namespace EPS.Core.Services.Implementations
{
    public class InputService : EPS.Foundation.IService
    {
#if ENABLE_INPUT_SYSTEM && STARTER_ASSETS_PACKAGES_CHECKED
        private PlayerInput _playerInput;
#endif
        
        private PlayerActions _inputActions;

        public PlayerActions GetInputActions()
        {
            return _inputActions;
        }

        public PlayerInput GetPlayerInput()
        {
            return _playerInput;
        }
        
        public string ReferencedName()
        {
            
            return this.GetType().ToString();
        }

        public void OnInit(DependencyManager manager)
        {
            _inputActions = Object.FindObjectOfType<PlayerActions>();
#if ENABLE_INPUT_SYSTEM && STARTER_ASSETS_PACKAGES_CHECKED
            _playerInput = Object.FindObjectOfType<PlayerInput>();
#else
			Debug.LogError( "Starter Assets package is missing dependencies. Please use Tools/Starter Assets/Reinstall Dependencies to fix it");
#endif
        }

        public void Loop() { }

        public void OnDestroy()
        {
            throw new NotImplementedException();
        }

        public void OnReset()
        {
            throw new NotImplementedException();
        }

        public Func<IEnumerator> LoopCourrutine()
        {
            return null;
        }
    }
}