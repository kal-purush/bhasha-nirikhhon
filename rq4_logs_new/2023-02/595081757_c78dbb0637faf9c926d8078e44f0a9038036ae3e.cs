using Events;
using GameConstants;
using UnityEngine;

namespace Puzzle
{
    public class EventTrigger : MonoBehaviour
    {
        [SerializeField] private DefaultEvent eventToTrigger;

        private bool _hasTriggered;

        private void OnTriggerEnter(Collider other)
        {
            if (!other.CompareTag(Tags.PlayerTag) && !other.CompareTag(Tags.GhostTag))
            {
                return;
            }

            if (_hasTriggered)
            {
                return;
            }

            eventToTrigger.RaiseEvent();
            _hasTriggered = true;
        }
    }
}