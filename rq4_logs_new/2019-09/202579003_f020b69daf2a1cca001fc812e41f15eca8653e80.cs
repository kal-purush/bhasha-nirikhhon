using UnityEngine;
using Utils;

namespace Interactibles.Teleporters
{
    [RequireComponent(typeof(Collider2D))]
    public class Teleporter : MonoBehaviour
    {
        [SerializeField] private Transform _exitPosition;
        [SerializeField] private Teleporter _exitNearbyTeleporter;
        [SerializeField] private bool _targetOnlyPlayer = true;
        [SerializeField] private float _disableAfterTeleportTime = 1f;

        private float _currentDisableTime;
        private Collider2D _collider;

        #region Unity Functions

        private void Start() => _collider = GetComponent<Collider2D>();

        private void Update()
        {
            if (_currentDisableTime > 0)
            {
                _currentDisableTime -= Time.deltaTime;

                if (_currentDisableTime <= 0)
                {
                    _collider.enabled = true;
                }
            }
        }

        private void OnTriggerEnter2D(Collider2D other)
        {
            if (_targetOnlyPlayer)
            {
                if (!other.CompareTag(TagManager.Player))
                {
                    return;
                }

                TeleportObjectAndStartTimer(other.transform);
            }
            else
            {
                TeleportObjectAndStartTimer(other.transform);
            }
        }

        #endregion

        #region External Functions

        public void DisableTeleporter()
        {
            _currentDisableTime = _disableAfterTeleportTime;
            _collider.enabled = false;
        }

        #endregion

        #region Utility Functions

        private void TeleportObjectAndStartTimer(Transform other)
        {
            other.position = _exitPosition.position;
            _currentDisableTime = _disableAfterTeleportTime;
            _collider.enabled = false;

            _exitNearbyTeleporter?.DisableTeleporter();
        }

        #endregion
    }
}