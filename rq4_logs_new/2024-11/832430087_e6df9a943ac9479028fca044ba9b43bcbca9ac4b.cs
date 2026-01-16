///<summary>
/// Author: Halen
///
/// Matches the rotation of this object to another one
///
///</summary>

using UnityEngine;

namespace Millivolt
{
	public class RotationMatcher : MonoBehaviour
	{
		[SerializeField] private Transform m_target;
        [SerializeField, Min(0)] private float m_time;

        private bool m_isTurning = false;
        private float m_lerpTime;

        private Vector3 m_initalUp;

        private void Update()
        {
            if (!m_isTurning && transform.up != m_target.up)
            {
                m_isTurning = true;
                m_initalUp = transform.up;
            }

            if (!m_isTurning)
                return;
            
            m_lerpTime += Time.deltaTime;
            if (m_lerpTime > m_time)
                m_lerpTime = m_time;

            float progress = m_lerpTime / m_time;

            if (progress == 1)
            {
                m_isTurning = false;
                transform.up = m_target.up;
            }
            else
                transform.up = Vector3.Lerp(m_initalUp, m_target.up, progress);
        }
    }
}