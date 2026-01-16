using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class N_CameraEvent : MonoBehaviour
{

    public float speed = 2f;

    private Camera thisCamera;
    private Vector3 worldDefalutForward;

    // Start is called before the first frame update
    void Start()
    {
        thisCamera = GetComponent<Camera>();
        worldDefalutForward = transform.forward;
    }

    // Update is called once per frame
    private void Update()
    {
        float scroll = Input.GetAxis("Mouse ScrollWheel") * speed;

        // 최대 줌인
        if (thisCamera.orthographicSize <= 3.0f && scroll < 0)
        {
            thisCamera.orthographicSize = 3.0f;
        }
        // 최대 줌아웃
        else if (thisCamera.orthographicSize >= 7.0f && scroll > 0)
        {
            thisCamera.orthographicSize = 7.0f;
        }
        // 줌인/줌아웃
        else
        {
            thisCamera.orthographicSize += scroll;
        }

        // 원상태로 돌아가기
        if (Input.GetKey(KeyCode.R))
        {
            thisCamera.orthographicSize = 5;
        }
    }

}