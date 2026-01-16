using UnityEngine;

namespace Assets.Scripts.Constructions.ConstructionCrane
{
    public class ConstructionCraneModel
    {
        public Camera WorldCamera;

        public Transform SpawnBuilding(Transform buildingToBuild)
        {
            Transform localBuilding = default;

            if (CastRayFromScreen(out RaycastHit hit))
            {
                localBuilding = Object.Instantiate(buildingToBuild, hit.point, Quaternion.identity);
                
                localBuilding.GetComponent<Collider>().isTrigger = true;
            }

            return localBuilding;
        }

        public bool CanBeBuilt(Transform currentBuilding)
        {
            Collider c = currentBuilding.GetComponent<Collider>();
            Vector3 constructionCenter = c.bounds.center;
            float sphereRadius = c.bounds.size.x / 2;
            Vector3 topHalfCenter = new Vector3(constructionCenter.x, c.bounds.max.y - sphereRadius, constructionCenter.z);
            Vector3 botHalfCenter = new Vector3(constructionCenter.x, c.bounds.min.y + sphereRadius, constructionCenter.z);
            bool canBuild = true;

            RaycastHit[] heathens = Physics.SphereCastAll(topHalfCenter, sphereRadius, Vector3.forward, sphereRadius);
            RaycastHit[] heathens2 = Physics.SphereCastAll(botHalfCenter, sphereRadius, Vector3.forward, sphereRadius);

            foreach (RaycastHit heathen in heathens)
            {
                //                Debug.Log(heathen.transform.gameObject.name + " " + CurrentBuilding.gameObject.name);
                if (heathen.transform.gameObject.name.Equals(currentBuilding.gameObject.name))
                {
                    canBuild = false;
                }
                //                Debug.Log("HIT " + heathen.transform.gameObject.name);
            }

            foreach (RaycastHit heathen in heathens2)
            {
                //                Debug.Log(heathen.transform.gameObject.name + " " + CurrentBuilding.gameObject.name);
                if (heathen.transform.gameObject.name.Equals(currentBuilding.gameObject.name))
                {
                    canBuild = false;
                }
                //                Debug.Log("HIT " + heathen.transform.gameObject.name);
            }

            //            if (CastRayFromScreen(out RaycastHit hit))
            //            {
            //                if (hit.transform.position.y != )
            //            }

            return canBuild;
        }

        public bool DoesTouchTheBarge(Transform centralBarge, Transform currentBuilding)
        {
            Collider someCollider = centralBarge.GetComponent<Collider>();
            Collider buildingCollider = currentBuilding.GetComponent<Collider>();

            if (someCollider && buildingCollider)
            {
                Vector3 botCenter = buildingCollider.bounds.center;
                botCenter.y = buildingCollider.bounds.min.y;

                Vector3 closestPoint = someCollider.ClosestPoint(botCenter);

//                Debug.Log(closestPoint + " " + Mathf.Abs(Vector3.Distance(botCenter, closestPoint)));
                if (Mathf.Abs(Vector3.Distance(botCenter, closestPoint)) < 10)
                {
                    return true;
                }
            }

            return false;
        }

        protected bool CastRayFromScreen(out RaycastHit hit)
        {
            Ray cameraRay = WorldCamera.ScreenPointToRay(Input.mousePosition);

            return Physics.Raycast(cameraRay, out hit, 1000f);
        }
    }
}