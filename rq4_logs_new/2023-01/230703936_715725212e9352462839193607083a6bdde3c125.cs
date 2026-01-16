using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using UnityEngine;

namespace JSI
{
    class VisibilityEnabler : MonoBehaviour
    {
        private Behaviour m_behaviour;

        public void Initialize(Behaviour behaviour)
        {
            m_behaviour = behaviour;
        }

        void OnBecameVisible()
        {
            m_behaviour.enabled = true;
        }

        void OnBecameInvisible()
        {
            m_behaviour.enabled = false;
        }
    }
}