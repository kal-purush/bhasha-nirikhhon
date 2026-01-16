using System;
using UnityEngine;

namespace WorldDisplay
{
    public class MakeChildrenFall : MonoBehaviour
    {
        [SerializeField] private Rigidbody2D[] _children;
        [SerializeField] private float _downwardInitialForce;
        [SerializeField] private bool _triggerOnCollide;

        #region Unity Functions

        private void Start()
        {
            foreach (Rigidbody2D child in _children)
            {
                child.isKinematic = true;
            }
        }

        private void OnTriggerEnter2D(Collider2D other)
        {
            if (_triggerOnCollide)
            {
                ThrowChildrenWithForce();
            }
        }

        #endregion

        #region External Functions

        public void ThrowChildrenWithForce()
        {
            foreach (Rigidbody2D child in _children)
            {
                child.isKinematic = false;
                child.AddForce(Vector2.down * _downwardInitialForce, ForceMode2D.Impulse);
            }

            Destroy(gameObject);
        }

        #endregion
    }
}