using Cinemachine;
using UnityEngine;
using Utilities;

namespace Platformer
{
    /**
     * <summary>
     * Class that handles camera shake.
     * </summary>
     */
    [RequireComponent(typeof(CinemachineVirtualCamera), typeof(CinemachineBasicMultiChannelPerlin))]
    public class CameraShake : MonoBehaviour
    {
        #region References
        private CinemachineVirtualCamera _cinemachineVirtualCamera;

        private CountdownTimer _shakeTimer;
        private CinemachineBasicMultiChannelPerlin _cinemachinePerlin;
        #endregion

        [Header("Parameters")]
        [SerializeField] private float _shakeIntensity = 1.5f;
        [SerializeField] private float _shakeTime = 0.2f;

        private void Awake()
        {
            _cinemachineVirtualCamera = GetComponent<CinemachineVirtualCamera>();
            _cinemachinePerlin = _cinemachineVirtualCamera.GetCinemachineComponent<CinemachineBasicMultiChannelPerlin>();
            _shakeTimer = new CountdownTimer(_shakeTime);
        }

        public void ShakeCamera()
        {
            ShakeCamera(_shakeIntensity, _shakeTime);
        }

        public void ShakeCamera(float intensity)
        {
            ShakeCamera(intensity, _shakeTime);
        }

        public void ShakeCamera(float intensity, float time)
        {
            _cinemachinePerlin.m_AmplitudeGain = intensity;

            _shakeTimer.Reset(time);
            _shakeTimer.Start();
        }

        public void StopShake()
        {
            _cinemachinePerlin.m_AmplitudeGain = 0;
            _shakeTimer.Stop();
        }

        void Update()
        {
            _shakeTimer.Tick(Time.deltaTime);

            if (_shakeTimer.IsFinished)
            {
                StopShake();
            }
        }
    }
}