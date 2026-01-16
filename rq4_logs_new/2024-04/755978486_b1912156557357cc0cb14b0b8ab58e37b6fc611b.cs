using System;
using System.Collections;
using System.Collections.Generic;
using Cinemachine;
using UnityEngine;
using AYellowpaper.SerializedCollections;
using UnityEngine.SceneManagement;


namespace Assets.Scripts.Effect
{
    public class ScreenShake : MonoBehaviour
    {
        public static ScreenShake Instance { get; private set; }
        private CinemachineImpulseSource _impSrc;

        #region ShakeParams

        [Serializable]
        public struct ShakeParams
        {
            public float intensity;
            public float seconds;

            public static ShakeParams QuickShake(float shakeLevel)
            {
                return new ShakeParams { intensity = shakeLevel * 0.1f, seconds = shakeLevel * 0.15f };
            }
        }

        [Serializable]
        public enum ShakeType
        {
            Attack,
            HitEnemy,
            TakeDamage
        }

        private static SerializedDictionary<ShakeType, ShakeParams> _shakeParams = new SerializedDictionary<ShakeType, ShakeParams> {
        { ShakeType.Attack, ShakeParams.QuickShake(0.2f) },
        { ShakeType.HitEnemy, ShakeParams.QuickShake(0.6f) },
        { ShakeType.TakeDamage, ShakeParams.QuickShake(2f) }
    };

        #endregion

        void Start()
        {
            // Singleton pattern
            if (Instance && Instance != this && Instance.gameObject.scene == SceneManager.GetActiveScene())
            {
                Destroy(this.gameObject);
                return;
            }
            else
            {
                Instance = this;
                _impSrc = Instance.GetComponent<CinemachineImpulseSource>();
            }
        }

        public static void Shake(ShakeParams shakeParams)
        {
            Instance._impSrc.m_ImpulseDefinition.m_ImpulseDuration = shakeParams.seconds;
            Instance._impSrc.GenerateImpulse(shakeParams.intensity);
        }

        public static void Shake(ShakeType shakeType)
        {
            Shake(_shakeParams[shakeType]);
        }
    }
}