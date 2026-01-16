using System;
using UnityEngine;

namespace Sandbox.Victor
{
    public class FireModeModifier : MonoBehaviour
    {
        [SerializeField]
        private FireMode newFireMode;
        [SerializeField]
        private bool removeOnContact;

        private void OnTriggerEnter( Collider other )
        {
            if ( !other.tag.Equals("Player") ) return;
            if ( !other.TryGetComponent(out ComponentWeapon weapon) ) return;

            weapon.ChangeFireMode(newFireMode);

            if ( removeOnContact )
                Destroy(gameObject);
        }
    }
}