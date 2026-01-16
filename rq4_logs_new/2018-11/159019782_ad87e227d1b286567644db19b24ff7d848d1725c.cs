using System.Collections;
using System.Collections.Generic;
using UnityEngine;

namespace FPS
{
    public class MagicArrow : BaseAmmo
    {
        [SerializeField]
        private float _destroyTime = 10f;
        [SerializeField]
        private LayerMask _layerMask;

        private float _speed;
        private bool _isHitted;

        public override void Initialize(Transform firepoint, float force)
        {
            Destroy(gameObject, _destroyTime);

            transform.position = firepoint.position;
            transform.rotation = firepoint.rotation;
            _speed = force;
        }

        private void FixedUpdate()
        {
            if (_isHitted) return;

            // позиция + вектор направления движения стрелы(возвращает единичный вектор, что это?) * скорость * Time.физический апдейт
            var finalPos = transform.position + transform.forward * _speed * Time.fixedDeltaTime;

            RaycastHit hit;
            if (Physics.Linecast(transform.position, finalPos, out hit, _layerMask))
            {
                _isHitted = true;
                transform.position = hit.point;

                //Наносим урон

                Destroy(gameObject, 1f);
            }
            else
            {
                transform.position = finalPos;
            }
        }
    }
}