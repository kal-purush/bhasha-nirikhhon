using Interactibles.ColliderModifier;
using UnityEngine;
using Utils;

namespace Collectibles
{
    public class CollectiblesCollector : MonoBehaviour
    {
        [SerializeField] private CollisionNotifier _collectibleNotifier;
        public ParticleSystem _collectibleSystem;
        public ParticleSystem _collectEffect;

        #region Unity Functions

        private void Start()
        {
            _collectibleNotifier.OnTriggerEntered += HandlePlayerCollision;
        }

        #endregion

        #region Utility Functions

        private void HandlePlayerCollision(Collider2D other)
        {
            if (other.CompareTag(TagManager.Player))
            {
                _collectibleNotifier.OnTriggerEntered -= HandlePlayerCollision;

                Destroy(_collectibleNotifier.gameObject);

                _collectibleSystem.Stop(true, ParticleSystemStopBehavior.StopEmitting);
                _collectEffect.Play();
            }

        }

        #endregion
    }
}