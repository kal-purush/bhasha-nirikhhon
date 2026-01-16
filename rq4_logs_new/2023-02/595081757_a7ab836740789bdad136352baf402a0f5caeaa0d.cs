using GameConstants;
using UnityEngine;

namespace Movement
{
    public class FloatHeightTrigger : MonoBehaviour
    {
        [SerializeField] private Vector2 newFloatRange;
    
        private void OnTriggerEnter(Collider other)
        {
            if (!other.CompareTag(Tags.GhostTag))
            {
                return;
            }

            var ghostFloatController = other.GetComponent<GhostMovement>();
            ghostFloatController.floatRange.x = newFloatRange.x;
            ghostFloatController.floatRange.y = newFloatRange.y;
        }
    }
}