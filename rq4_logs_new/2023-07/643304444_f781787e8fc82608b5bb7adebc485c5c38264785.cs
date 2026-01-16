using UnityEngine;

namespace KamranWali.CodeOptPro.Searches
{
    public class RayHit
    {
        protected int hitSize = 1;
        protected float rayRange;
        protected LayerMask hitMask;
        protected Ray ray;
        protected RaycastHit[] raycastHit;
        protected int numHit;

        /// <summary>
        /// This constructor initializes the RayHit.
        /// </summary>
        /// <param name="hitSize">The size of the hit array, of type int</param>
        /// <param name="rayRange">The range of the ray, of type float</param>
        /// <param name="hitMask">The hitting layers, of type LayerMask</param>
        public RayHit(int hitSize, float rayRange, LayerMask hitMask)
        {
            this.hitSize = hitSize <= 0 ? 1 : hitSize;
            this.rayRange = rayRange;
            this.hitMask = hitMask;
            ray = new Ray();
            raycastHit = new RaycastHit[hitSize];
        }

        /// <summary>
        /// This method sets the ray.
        /// </summary>
        /// <param name="ray">The ray to set, of type Ray</param>
        public void SetRay(Ray ray) => this.ray = ray;

        /// <summary>
        /// This method sets the ray.
        /// </summary>
        /// <param name="origin">The origin point of the ray, of type Vector3</param>
        /// <param name="dir">The direction of the ray, of type Vector3</param>
        public void SetRay(Vector3 origin, Vector3 dir)
        {
            ray.origin = origin;
            ray.direction = dir;
        }

        /// <summary>
        /// This method calculates all the hit objects with the ray.
        /// </summary>
        public virtual void CalculateHits() => numHit = Physics.RaycastNonAlloc(ray, raycastHit, rayRange, hitMask);

        /// <summary>
        /// This method gets all the hits with the ray.
        /// </summary>
        /// <returns>An array of all the hits, of type RaycastHit[]</returns>
        public virtual RaycastHit[] GetAllHits() => raycastHit;

        /// <summary>
        /// This method checks if the ray has hit atleast 1 object.
        /// </summary>
        /// <returns>True means hit found, false otherwise, of type bool</returns>
        public bool IsHit() => numHit != 0;

        /// <summary>
        /// This method gets the number of objects hit by the ray.
        /// </summary>
        /// <returns>The number of objects hit by the ray, of type int</returns>
        public int GetNumHit() => numHit;
    }
}