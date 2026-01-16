using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public static class MathUtils
{
    public static class Vector
    {
        /// <summary>
        /// Given a rotation, returns a unit vector pointing in the same direction.
        /// </summary>
        /// <param name="rotation">Rotation to point along.</param>
        /// <returns>Unit vector pointing the same way as <paramref name="rotation"/>.</returns>
        public static Vector3 FromRotation(Quaternion rotation)
        {
            return rotation * Vector3.forward;
        }

        public static Vector3 FromRotation(Vector3 rotation)
        {
            return FromRotation(Quaternion.Euler(rotation));
        }
    }
}