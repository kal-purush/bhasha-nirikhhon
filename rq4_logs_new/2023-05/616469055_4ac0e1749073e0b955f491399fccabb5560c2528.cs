using System;
using System.Collections.Generic;
using System.Linq;
using UnityEngine;
using Random = UnityEngine.Random;

namespace Runtime.CameraSystems
{
    public class CameraShake : MonoBehaviour
    {
        private readonly List<CameraShakeData> _shakeHolder = new();
        
        private Vector3 _positionOrigin;

        private void Awake()
        {
            _positionOrigin = transform.localPosition;
        }

        public void Shake(CameraShakeData data)
        {
            _shakeHolder.Add(data);
        }

        private void Update()
        {
            _shakeHolder.ForEach(data =>
            {
                data.UpdateShake(Time.deltaTime);
            });

            _shakeHolder.RemoveAll(data => data.DurationLeft <= 0);
            
            float positionStrength = _shakeHolder.Count > 0 ? _shakeHolder.Max(data => data.PositionStrength): 0;

            if (positionStrength <= 0)
            {
                transform.localPosition = _positionOrigin;
            }
            else
            {
                Vector3 toAdd = Random.insideUnitSphere * positionStrength;
                transform.localPosition = _positionOrigin + toAdd;
            }
        }
    }
}