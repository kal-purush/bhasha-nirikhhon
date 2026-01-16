using System.Collections;
using System.Collections.Generic;
using UnityEngine;

namespace Petzak
{
    public class Bridge : MonoBehaviour
    {
        // Start is called before the first frame update
        void Start()
        {
            //gameObject.transform.transform.Rotate(0, 0, 90, Space.World);
            //gameObject.transform.transform.Translate(-13, -13, 0, Space.World);
        }

        // Update is called once per frame
        void Update()
        {
            if (Game.main.LowerBridge && !Game.main.BridgeLowered)
            {
                Game.main.BridgeLowered = true;
                gameObject.transform.transform.Rotate(0, 0, 90, Space.World);
                gameObject.transform.transform.Translate(-13, -13, 0, Space.World);
            }
        }
    }

}