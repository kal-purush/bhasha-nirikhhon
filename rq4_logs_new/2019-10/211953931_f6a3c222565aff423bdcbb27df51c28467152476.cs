using System.Collections;
using System.Collections.Generic;
using UnityEngine;

namespace Petzak
{
    public class Arrow : MonoBehaviour
    {
        void OnMouseDown()
        {
            gameObject.SetActive(false);

            //if (Camera.current.transform.position.x < -80)
            //    Camera.current.transform.Translate(0, 0, 1);

            print("asdf");
        }

        void Start()
        {

        }

        void Update()
        {
            //if (Camera.current.transform.position.x < -80)
            //    Camera.current.transform.Translate(0, 0, 1);
            //gameObject.transform.Translate(-1, 0, 0);
        }
    }
}