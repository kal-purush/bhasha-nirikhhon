using UnityEngine;

namespace EPS.GamePhysics.Vehicles.Helicopter
{
    public class EnginePhysics : MonoBehaviour
    {
        public float maxHorsePower = 140.0f;
        public float maxRpm= 2700.0f;
        public float powerDelay = 2f;
        private float _currentHorsePower;
        private float _currentRpm;
        public AnimationCurve powerCurve = new AnimationCurve(new Keyframe(0f, 0f), new Keyframe(1f, 1f));

        public float CurrentHorsePower => _currentHorsePower;

        public float CurrentRpm => _currentRpm;

        public void UpdateEngine(bool throttle)
        {
            // Calculate horsepower
            float wantedHorsePower = throttle ? maxHorsePower : 0f;
            _currentHorsePower = Mathf.Lerp(_currentHorsePower, wantedHorsePower, Time.deltaTime * powerDelay);

            // Calculate RPM's
            float wantedRpm = throttle ? maxRpm : 0f;
            _currentRpm = Mathf.Lerp(_currentRpm, wantedRpm, Time.deltaTime * powerDelay);
        }
    }
}