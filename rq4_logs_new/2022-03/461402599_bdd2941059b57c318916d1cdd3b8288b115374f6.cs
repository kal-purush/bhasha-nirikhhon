using System.Collections.Generic;
using System.Linq;
using UnityEngine;

namespace Basements
{
    class Basement : MonoBehaviour
    {
        public Bounds interiorBounds;
        public Collider[] localColliders;

        public static List<Basement> allBasements = new List<Basement>();
        public static List<Basement> nestedBasements = new List<Basement>();

        GameObject b;

        public string mUID;
        public int nestedCount;
        void Awake()
        {
            
            allBasements.Add(this);

            localColliders = gameObject.GetComponentsInChildren<Collider>();

            b = transform.Find("Interior/Bounds").gameObject;
            b.layer = 4; // Allows building without disabling zone detection, idk what this layer is actually for
            interiorBounds = b.GetComponent<BoxCollider>().bounds;
        }

        private void OnEnable()
        {
            mUID = System.Guid.NewGuid().ToString();
            var test = Physics.OverlapSphere(interiorBounds.center, 1275f,4).Where(x => !localColliders.Contains(x));
            nestedCount = 0;
            foreach (var collider in test)
            {
                if (collider.gameObject.GetComponent<Basement>() == null) return;

                if (collider.gameObject.GetComponent<Basement>().mUID == mUID) break;
                nestedCount++;
                nestedBasements.Add(collider.gameObject.GetComponent<Basement>());
            }
        }
        void OnDestroy()
        {
            allBasements.Remove(this);
        }

        public bool CanBeRemoved()
        {
            var ol = Physics.OverlapBox(interiorBounds.center, interiorBounds.extents).Where(x => !localColliders.Contains(x));
            foreach (var item in ol)
            {
                Debug.Log(item.name + " is preventing basement from being destroyed");
            }
            return !ol.Any();            
        } 
    }
}