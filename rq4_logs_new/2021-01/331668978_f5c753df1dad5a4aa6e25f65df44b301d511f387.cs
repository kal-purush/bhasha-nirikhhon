using UnityEngine;

namespace Player
{
    [RequireComponent(typeof(Input))]
    public class InteractionController : MonoBehaviour
    {
        public LayerMask interactionMask;
        public float rerticleRadius, rerticleLength;
        private Input m_Input;
        // Start is called before the first frame update
        void Awake()
        {
            m_Input = GetComponent<Input>();
        }

        // Update is called once per frame
        void Update()
        {
           // Collider[] foundObjects = Physics.OverlapCapsule()
        }
    }
}