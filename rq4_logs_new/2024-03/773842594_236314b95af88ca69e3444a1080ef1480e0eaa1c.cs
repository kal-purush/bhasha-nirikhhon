using System;
using System.Collections.Generic;
using UnityEngine;

namespace hungtrinh
{
    public class Arrow3_ray : Arrow
    {
        [Header("DEBUG")] [SerializeField] [ReadOnlyInspector]
        bool isSeparation = false;

        [SerializeField]         float       scaleSpeedWithParent;
        [SerializeField]         int         numberArrowSeparation;
        [SerializeField]         Arrow       arrowSeparationPrefab;
        [HideInInspector] public List<Arrow> listArrowSpe;
        [SerializeField]         float       scaleWithParent;

        protected override void Update()
        {
            base.Update();

            if (Input.GetKeyDown(KeyCode.Space) && !isSeparation)
            {
                arrowInteracting = null;
                Separation();
            }
        }

        protected void Separation()
        {
            int force = Mathf.RoundToInt(numberArrowSeparation / 2f);

            var parentVelocity = this.rigid.velocity;

            for (int i = 0; i <= numberArrowSeparation; i++)
            {
                Debug.Log("Separation");
                var arrowSpe = Instantiate(arrowSeparationPrefab, transform.position, transform.rotation);
                arrowSpe.transform.position = transform.position;
                listArrowSpe.Add(arrowSpe);
                arrowSpe.gameObject.SetActive(true);
                force -= 1;

                parentVelocity.y              += force;
                arrowSpe.rigid.velocity       =  parentVelocity       * scaleSpeedWithParent;
                arrowSpe.transform.localScale =  transform.localScale * scaleWithParent;
            }

            gameObject.SetActive(false);

            isSeparation = true;
        }

        protected override void OnTriggerEnter2D(Collider2D collision)
        {
            base.OnTriggerEnter2D(collision);
        }
    }
}