using System.Collections;
using UnityEngine;

namespace Buffalo
{
    [RequireComponent(typeof(Rigidbody2D))]
    [RequireComponent(typeof(Collider2D))]
    public class CharacterController2D : Gamekit2D.CharacterController2D
    {
        [Tooltip("The Layer with which the character should not collide while phasing red.")]
        public string redLayerName;
        LayerMask m_redContactFilterMask;
        int m_redLayerId;

        float m_resonateTime = 3f;

        GameObject m_Player;

        void Awake()
        {
            if (m_Player == null)
                m_Player = GameObject.FindGameObjectWithTag("Player");

            m_Rigidbody2D = GetComponent<Rigidbody2D>();
            m_Capsule = GetComponent<CapsuleCollider2D>();

            m_CurrentPosition = m_Rigidbody2D.position;
            m_PreviousPosition = m_Rigidbody2D.position;

            m_ContactFilter.layerMask = groundedLayerMask;
            m_ContactFilter.useLayerMask = true;
            m_ContactFilter.useTriggers = false;

            LayerMask redLayerMask = LayerMask.GetMask(redLayerName);
            m_redContactFilterMask = (groundedLayerMask | redLayerMask) ^ redLayerMask;
            m_redLayerId = LayerMask.NameToLayer(redLayerName);

            Physics2D.queriesStartInColliders = false;
        }

        public void Resonate()
        {
            StartCoroutine(ResonateRed());
        }

        protected IEnumerator ResonateRed()
        {
            m_ContactFilter.layerMask = m_redContactFilterMask;
            Physics2D.IgnoreLayerCollision(m_Player.gameObject.layer, m_redLayerId, true);
            float resonateTime = m_resonateTime;

            while (resonateTime > 0) {
                resonateTime -= Time.deltaTime;
                yield return null;
            }

            m_ContactFilter.layerMask = groundedLayerMask;
            Physics2D.IgnoreLayerCollision(m_Player.gameObject.layer, m_redLayerId, false);
        }
    }
}