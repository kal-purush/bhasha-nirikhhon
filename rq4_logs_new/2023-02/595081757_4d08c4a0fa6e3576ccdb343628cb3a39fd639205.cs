using UnityEngine;
using UnityEngine.Serialization;

namespace UI.BlobShadow
{
    public class BlobShadow : MonoBehaviour
    {
        [SerializeField] private GameObject blobShadow;
    
        void Update()
        {
            Ray floorSeekingRay = new Ray(transform.position, -transform.up);
            if (Physics.Raycast(floorSeekingRay, out RaycastHit hit, 10f, 1 << LayerMask.NameToLayer("Floor")))
            {
                blobShadow.transform.position = hit.point;
                
                Vector3 lookAt = Vector3.Cross(-hit.normal, transform.right);
                // reverse it if it is down.
                lookAt = lookAt.y < 0 ? -lookAt : lookAt;
                blobShadow.transform.rotation = Quaternion.LookRotation(hit.point + lookAt, hit.normal);
            }
        }
    }
}