using UnityEngine;

namespace Interactibles.ColliderModifier
{
    [RequireComponent(typeof(Collider2D))]
    public class CollisionNotifier : MonoBehaviour
    {
        public delegate void TriggerEntered(Collider2D other);
        public delegate void TriggerExited(Collider2D other);

        public TriggerEntered OnTriggerEntered;
        public TriggerExited OnTriggerExited;

        #region Unity Functions

        private void OnTriggerEnter2D(Collider2D other) => OnTriggerEntered?.Invoke(other);

        private void OnTriggerExit2D(Collider2D other) => OnTriggerExited?.Invoke(other);

        #endregion
    }
}