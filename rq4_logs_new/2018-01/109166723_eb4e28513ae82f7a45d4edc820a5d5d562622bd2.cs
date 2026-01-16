using System.Collections;
using System.Collections.Generic;
using UnityEngine;

namespace Blane
{
    public class AerodynamicForce
    {

        public List<Triangle> _triangleList;

        public static Vector3 windDirection = new Vector3(0, 0, 2f);
        public static float windDensity = 2.5f;
        public static float drag = 3.5f;

        // Has to be static to be called in cloth update
        public static void Wind(Triangle currentTriangle)
        {
            ClothParticle triStart = currentTriangle.p1;
            ClothParticle triTop = currentTriangle.p2;
            ClothParticle triOpp = currentTriangle.p3;

            // Average velocity of particles
            Vector3 surface = (triStart.velocity + triTop.velocity + triOpp.velocity) / 3;
            Vector3 avgVelocity = surface - windDirection;

            // Triangle normal
            Vector3 triNormal = Vector3.Cross(triTop.position - triStart.position,
                                triOpp.position - triStart.position).normalized;

            // Triangle area
            float triArea = Vector3.Dot(avgVelocity, triNormal) / avgVelocity.magnitude;

            // Calculate force applied
            #region No?
            //Vector3 appliedForce = windDensity * (avgVelocity.magnitude * Vector3.Dot(avgVelocity, triNormal)) * drag * triArea * triNormal;
            #endregion
            Vector3 appliedForce = windDensity * (avgVelocity.magnitude * avgVelocity.magnitude) * triArea * triNormal;

            // Apply Force
            triStart.AddForce(appliedForce / 3);
            triTop.AddForce(appliedForce / 3);
            triOpp.AddForce(appliedForce / 3);

        }
    }
}