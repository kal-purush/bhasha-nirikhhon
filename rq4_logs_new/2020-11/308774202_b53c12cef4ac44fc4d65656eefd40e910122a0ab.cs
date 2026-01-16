
using UnityEngine;

namespace PersonalDevelopment
{
    public class PlayerAnimationBehaviour : MonoBehaviour
    {
        private const string IsRunningAnimationParameter = "IsRunning";
        private const string IsTauntingAnimationParameter = "IsTaunting";

        [SerializeField] private Animator _animator = null;

        public void SetRunParameter(bool isRunning)
        {
            _animator.SetBool(IsRunningAnimationParameter, isRunning);
        }

        public void SetTauntParameter(bool isTaunting)
        {
            _animator.SetBool(IsTauntingAnimationParameter, isTaunting);
        }
    }

}