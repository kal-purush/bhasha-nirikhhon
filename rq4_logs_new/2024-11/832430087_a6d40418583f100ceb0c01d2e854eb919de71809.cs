///<summary>
/// Author: Halen
///
/// Component to handle behaviour for how and when cutscenes are played.
///
///</summary>

using Cinemachine;
using System;
using System.Collections;
using UnityEngine;

namespace Millivolt
{
    namespace Cameras
    {
        public class CutsceneController : MonoBehaviour
        {
            [SerializeField] private CinemachineVirtualCamera[] m_vcams = new CinemachineVirtualCamera[0];

            [SerializeField, Min(0)] private float[] m_durations = new float[0];

            [SerializeField, Min(0)] private float m_returnControlDelay = 0.6f;

            public void StartCutscene()
            {
                StartCoroutine(Play());
            }

            private IEnumerator Play()
            {
                GameManager.Player.Controller.SetCanMove(Player.CanMoveType.Cutscene, false);

                for (int c = 0; c < m_vcams.Length; c++)
                {
                    m_vcams[c].Priority += 10;
                    yield return new WaitForSeconds(m_durations[c]);
                    m_vcams[c].Priority -= 10;
                }

                yield return new WaitForSeconds(m_returnControlDelay);
                GameManager.Player.Controller.SetCanMove(Player.CanMoveType.Cutscene, true);
            }

            private void OnValidate()
            {
                if (m_vcams.Length != m_durations.Length)
                {
                    float[] hold = m_durations;
                    m_durations = new float[m_vcams.Length];
                    Array.Copy(hold, m_durations, m_durations.Length < hold.Length ? m_durations.Length : hold.Length);
                }
            }
            private void OnDisable()
            {
                StopAllCoroutines();
            }
        }
    }
}