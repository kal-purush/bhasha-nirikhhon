// --------------------------------------------------------------------------------------------------------------------
// <copyright file="UxrShakeDetector.cs" company="ROVR">
//   Copyright (c) ROVR, All rights reserved.
// </copyright>
// --------------------------------------------------------------------------------------------------------------------
using System;
using System.Collections.Generic;
using UltimateXR.Avatar;
using UltimateXR.Core.Components.Composite;
using UnityEngine;

namespace UltimateXR.Mechanics
{
    /// <summary>
    ///     Detects the shaking of an object while being grabbed.
    ///     Usage: Register to the <see cref="OnShake" /> event to detect whether a grabbed object is being shaken.
    /// </summary>
    public class UxrShakeDetector : UxrGrabbableObjectComponent<UxrShakeDetector>
    {
        #region Inspector Properties/Serialized Fields

        [SerializeField] private float _shakeThreshold  = 1.0f;
        [SerializeField] private float _historyDuration = 0.5f;

        #endregion

        #region Unity

        private void Update()
        {
            // Check whether we're grabbing the object.
            if (!IsBeingGrabbed)
            {
                return;
            }

            _timeSinceLastEntry += Time.deltaTime;
            // Debug.Log(_timeSinceLastEntry);

            // If the time since last sample is greater than the set interval, we update histories.
            if (_timeSinceLastEntry >= HistoryInterval)
            {
                // Add the object's and player's positional data to their respective histories.
                // Debug.Log(transform.position);
                _positionHistory.Add(transform.position);
                _playerPositionHistory.Add(UxrAvatar.LocalAvatar.transform.position);

                // Reset the time since last sample.
                _timeSinceLastEntry = 0.0f;

                // Remove old entries if they exceed the history duration.
                if (_positionHistory.Count > Mathf.FloorToInt(_historyDuration / HistoryInterval))
                {
                    _positionHistory.RemoveAt(0);
                    _playerPositionHistory.RemoveAt(0);
                }

                // Detect shake after updating histories.
                DetectShake();
            }
        }

        #endregion

        #region Private Methods

        private void DetectShake()
        {
            var totalMovement = 0.0f;

            for (var i = 1; i < _positionHistory.Count; i++)
            {
                // Check object movement between two history items.
                Vector3 objectMovement = _positionHistory[i] - _positionHistory[i - 1];

                // Check player  movement between two of the player's history items.
                Vector3 playerMovement = _playerPositionHistory[i] - _playerPositionHistory[i - 1];

                // Subtract player's movement from the object's movement to create the relative movement.
                Vector3 relativeMovement = objectMovement - playerMovement;

                // Get the magnitude of said relative movement.
                totalMovement += relativeMovement.magnitude;
            }

            if (totalMovement >= _shakeThreshold)
            {
                OnShake?.Invoke();
            }
        }

        #endregion

        #region Public Types & Data

        public Action OnShake;

        #endregion

        #region Private Types & Data

        private const float HistoryInterval = 0.05f; // The interval between positional samples.

        private readonly List<Vector3> _positionHistory       = new List<Vector3>(); // History of the object's positions.
        private readonly List<Vector3> _playerPositionHistory = new List<Vector3>(); // History of the player's positions.

        private float _timeSinceLastEntry;

        #endregion
    }
}