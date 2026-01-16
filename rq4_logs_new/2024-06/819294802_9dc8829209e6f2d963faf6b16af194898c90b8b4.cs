using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class CameraController : MonoBehaviour
{
    public float turnSpeed = 4.0f; // 마우스 회전 속도    
    private float xRotate = 0.0f; // 내부 사용할 X축 회전량은 별도 정의 ( 카메라 위 아래 방향 )
    public float moveSpeed = 4.0f; // 이동 속도
    float MAX_RAY_DISTANCE = 80.0f;

    GameObject _selectedObject;
    Color _selectedObjectColor;

    // Start is called before the first frame update
    void Start()
    {
        Managers.Input.MouseAction -= OnMouseAction;
        Managers.Input.MouseAction += OnMouseAction;
        Managers.Input.KeyAction -= OnKeyAction;
        Managers.Input.KeyAction += OnKeyAction;
    }

    private void Update()
    {
        OnKeyUp();
    }

    // Update is called once per frame
    void OnMouseAction(Define.MouseEvent mouseEvent)
    {
        if(mouseEvent == Define.MouseEvent.Move)
        {
            // 좌우로 움직인 마우스의 이동량 * 속도에 따라 카메라가 좌우로 회전할 양 계산
            float yRotateSize = Input.GetAxis("Mouse X") * turnSpeed;
            // 현재 y축 회전값에 더한 새로운 회전각도 계산
            float yRotate = transform.eulerAngles.y + yRotateSize;

            // 위아래로 움직인 마우스의 이동량 * 속도에 따라 카메라가 회전할 양 계산(하늘, 바닥을 바라보는 동작)
            float xRotateSize = -Input.GetAxis("Mouse Y") * turnSpeed;
            // 위아래 회전량을 더해주지만 -45도 ~ 80도로 제한 (-45:하늘방향, 80:바닥방향)
            // Clamp 는 값의 범위를 제한하는 함수
            xRotate = Mathf.Clamp(xRotate + xRotateSize, -45, 80);

            // 카메라 회전량을 카메라에 반영(X, Y축만 회전)
            transform.eulerAngles = new Vector3(xRotate, yRotate, 0);
        }
       
    }
    void OnKeyAction()
    {
        //  키보드에 따른 이동량 측정
        Vector3 move =
            transform.forward * Input.GetAxis("Vertical") +
            transform.right * Input.GetAxis("Horizontal");

        // 이동량을 좌표에 반영
        Vector3 moveDistance = move * moveSpeed * Time.deltaTime;
        transform.position += moveDistance;

        if (Input.GetKey(KeyCode.Space))
        {
            if (!_selectedObject)
            {
                Ray ray = Camera.main.ViewportPointToRay(new Vector3(0.5f, 0.5f, 0f));
                Debug.DrawRay(Camera.main.transform.position, ray.direction * 100.0f, Color.red, 1.0f);
                RaycastHit hit;
                if (Physics.Raycast(ray, out hit, MAX_RAY_DISTANCE, LayerMask.GetMask("Object")))
                {
                    _selectedObject = hit.transform.gameObject;
                    _selectedObject.GetComponent<Rigidbody>().useGravity = false;
                    _selectedObjectColor = _selectedObject.GetComponent<Renderer>().material.color;
                    _selectedObject.GetComponent<Renderer>().material.SetColor("_Color",
                        new Color(0.0f, 0.0f, 1.0f, 0.1f));
                    _selectedObject.GetComponent<BoxCollider>().isTrigger = true;
                }
            }
            else
            {
                _selectedObject.transform.position += moveDistance;
            }
        }
        
    }

    void OnKeyUp()
    {
        if (Input.GetKeyUp(KeyCode.Space) && _selectedObject)
        {
            _selectedObject.GetComponent<Rigidbody>().useGravity = true;
            _selectedObject.GetComponent<Renderer>().material.SetColor("_Color",
                        _selectedObjectColor);
            _selectedObject.GetComponent<BoxCollider>().isTrigger = false;
            _selectedObject = null;
        }
    }
}