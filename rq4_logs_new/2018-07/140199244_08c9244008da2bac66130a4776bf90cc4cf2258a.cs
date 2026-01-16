
using System.Collections;
using System.Collections.Generic;
using UnityEngine;

namespace CompleteProject
{
    public class PlayerShield : MonoBehaviour
    {
        Rigidbody getRigidBody;
        public GameObject Bubble;
        private GameObject Shield;
        public bool Orb;
        //public EnemyAttack Ea;
        // Use this for initialization
        void Awake()
        {
            getRigidBody = GetComponent<Rigidbody>();
            Orb = false;
        }
        
        //Update is called once per frame
        //void Update()
        //{
        //    if (Ea != null)
        //    {
        //        if (Orb == true)
        //        {
        //            Ea.attackDamage = 0;
        //        }
        //        else if (Orb == false)
        //       {
        //            Ea.attackDamage = 10;
        //        }
        //   }
        //}

        void OnTriggerEnter(Collider trig)
        {
            if (trig.gameObject.tag == "Fire" || trig.gameObject.tag == "Ice" || trig.gameObject.tag == "Earth")
            {
                Orb = true;
                Shield = Instantiate(Bubble, getRigidBody.transform.position, Quaternion.identity);
                Shield.transform.parent = getRigidBody.transform;
                Debug.Log("Object Entered The Trigger");
            }
        }

        void OnTriggerExit(Collider trig)
        {
            if (trig.gameObject.tag == "Fire" || trig.gameObject.tag == "Ice" || trig.gameObject.tag == "Earth")
            {
                Orb = false;
                Destroy(Shield, 0f);
				Bubble.SetActive (false);
                Debug.Log("Object Exited the Trigger");

            }
        }   
    }   
}