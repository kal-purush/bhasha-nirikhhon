using UnityEngine;


namespace JevLogin
{
    public sealed class Observer : MonoBehaviour
    {
        public Transform player;

        bool m_IsPlayerInRange;

        private void Start()
        {
            player = FindObjectOfType<PlayerBase>().transform;
        }

        void OnTriggerEnter(Collider other)
        {
            if (other.transform.Equals(player))
            {
                m_IsPlayerInRange = true;
            }
        }

        void OnTriggerExit(Collider other)
        {
            if (other.transform.Equals(player))
            {
                m_IsPlayerInRange = false;
            }
        }

        void Update()
        {
            if (m_IsPlayerInRange)
            {
                Vector3 direction = player.position - transform.position + Vector3.up;
                Ray ray = new Ray(transform.position, direction);
                RaycastHit raycastHit;

                if (Physics.Raycast(ray, out raycastHit))
                {
                    if (raycastHit.collider.transform == player)
                    {

                    }
                }
            }
        }
    }
}