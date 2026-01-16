using Sirenix.OdinInspector;
using UnityEngine;

namespace PersonalDevelopment {
    public class StateDebugger : MonoBehaviour
    {

        [ShowInInspector]
        [ReadOnly]
        public State GameState => StateManager.Instance.GetState();

        
        [Button(ButtonSizes.Medium), GUIColor(0.4f, 0.8f, 1f)]
        public void Play()
        {
            StateManager.Instance.SetState(State.Play);
        }

        [Button(ButtonSizes.Medium), GUIColor(0.906f, 0.6f, 0.2f)]
        public void Pause()
        {
            StateManager.Instance.SetState(State.PreGame);
        }
    }
}