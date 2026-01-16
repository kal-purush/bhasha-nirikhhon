using UnityEngine;
using Photon.Pun;

public class MouseHandlerJose : MonoBehaviour
{
    // horizontal rotation speed
    public float horizontalSpeed = 1f;
    // vertical rotation speed
    public float verticalSpeed = 1f;
    private float xRotation = 0.0f;
    private float yRotation = 0.0f;
    //private Camera cam;

    private PhotonView pw;

    void Start()
    {
        pw = GetComponent<PhotonView>();
        if (pw.IsMine) {
            Camera.main.transform.SetParent(this.transform);
            Camera.main.transform.position = this.transform.position + new Vector3(0, 0.6f, 0.07f);
        }
        
    }

    void Update()
    {
        if (pw.IsMine)
        {
            float mouseX = Input.GetAxis("Mouse X") * horizontalSpeed;
            float mouseY = Input.GetAxis("Mouse Y") * verticalSpeed;

            yRotation += mouseX;
            xRotation -= mouseY;
            xRotation = Mathf.Clamp(xRotation, -90, 90);

            Camera.main.transform.eulerAngles = new Vector3(xRotation, yRotation, 0.0f);
            transform.eulerAngles = new Vector3(0.0f, yRotation, 0.0f);
        }
    }
}