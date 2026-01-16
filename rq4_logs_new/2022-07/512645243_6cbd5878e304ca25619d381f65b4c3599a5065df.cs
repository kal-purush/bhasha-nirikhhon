using System.Collections;
using System.Collections.Generic;
using UnityEngine;

namespace EPS
{
    [System.Serializable]
    public class RecoilConfig
    {
        public AnimationCurve kickBackCurve;
        public float kickBackDistance = -1; // backwards
        public float recoilSpringForce = 1;
        public float pitchKickBackAngleRange = 10;
        public float yawKickBackAngleRange = 30;
        //OPTIONAL
        public Transform bolt;
        public float boltKickBackRange =-2; // backwards
    }
}