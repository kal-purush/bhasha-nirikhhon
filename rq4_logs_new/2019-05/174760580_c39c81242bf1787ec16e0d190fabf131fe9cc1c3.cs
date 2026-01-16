using System.Collections;
using UnityEngine;

namespace Buffalo
{
    public class Resonator : MonoBehaviour
    {
        [Tooltip("The Layer with which the character should not collide while phasing red.")]
        public string redLayerName;
        LayerMask m_redLayerMask;
        int m_redLayerId;

        float m_resonateTime = 3f;

        static Resonator s_Instance;
        static Resonator Instance
        {
            get
            {
                if (s_Instance != null)
                {
                    return s_Instance;
                }

                s_Instance = FindObjectOfType<Resonator>();

                if (s_Instance != null)
                {
                    return s_Instance;
                }

                Create();

                return s_Instance;
            }
            set { s_Instance = value; }
        }

        static void Create()
        {
            GameObject resonatorGameObject = new GameObject("Resonator");
            s_Instance = resonatorGameObject.AddComponent<Resonator> ();
        }

        void Awake()
        {
            if (Instance != this)
            {
                Destroy(gameObject);
                return;
            }

            m_redLayerMask = LayerMask.GetMask(redLayerName);
            m_redLayerId = LayerMask.NameToLayer(redLayerName);
        }

        public static void Resonate()
        {
            Instance.StartCoroutine(Instance.ResonateRed());
        }

        protected IEnumerator ResonateRed()
        {
            ContactFilter2D contactFilter = PlayerCharacter.PlayerInstance.characterController.ContactFilter;
            GameObject player = PlayerCharacter.PlayerInstance.gameObject;
            LayerMask groundedLayerMask = PlayerCharacter.PlayerInstance.characterController.groundedLayerMask;

            contactFilter.layerMask = (groundedLayerMask | m_redLayerMask) ^ m_redLayerMask;
            Physics2D.IgnoreLayerCollision(player.layer, m_redLayerId, true);
            float resonateTime = m_resonateTime;

            while (resonateTime > 0)
            {
                resonateTime -= Time.deltaTime;
                yield return null;
            }

            contactFilter.layerMask = groundedLayerMask;
            Physics2D.IgnoreLayerCollision(player.layer, m_redLayerId, false);
        }
    }
}