using UnityEngine;
using System.Collections;

public class DollyZoom : MonoBehaviour
{
    public Transform target;
    public Camera cameraOption;

    private float initHeightAtDist;
    public bool dzEnabled;
    public bool dollyZoomIn;

    float FrustumHeightAtDistance(float distance)
    {
        return 2.0f * distance * Mathf.Tan(cameraOption.fieldOfView * 0.5f * Mathf.Deg2Rad);
    }

    public float FOVForHeightAndDistance(float height, float distance)
    {
        return 2.0f * Mathf.Atan(height * 0.5f / distance) * Mathf.Rad2Deg;
    }

    // Dolly 줌 시작
    public void StartDZ()
    {
        var distance = Vector3.Distance(transform.position, target.position);
        initHeightAtDist = FrustumHeightAtDistance(distance);
        dzEnabled = true;
    }

    // Dolly 줌 종료
    public void StopDZ()
    {
        dzEnabled = false;
    }

    void Start()
    {
        StartDZ();
    }

    void Update()
    {
        if (dzEnabled)
        {
            // 시야각과 카메라와 타겟간의 거리 계산 
            var currDistance = Vector3.Distance(this.transform.position, target.position);
            cameraOption.fieldOfView = FOVForHeightAndDistance(initHeightAtDist, currDistance);
        }



        if (dollyZoomIn && cameraOption.transform.position.z <= -5)
        {
            transform.Translate(Vector3.forward * Time.deltaTime * 10f);
        }
        else if (!dollyZoomIn && cameraOption.transform.position.z >= -15)
        {
            transform.Translate(Vector3.forward * Time.deltaTime * -10f);
        }
    }
}