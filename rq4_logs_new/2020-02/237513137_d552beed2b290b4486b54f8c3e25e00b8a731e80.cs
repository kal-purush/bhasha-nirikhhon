using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class PlayerCam : MonoBehaviour
{
    [SerializeField]
    private float mouseSensitivity = 100f;
    [SerializeField]
    private GameObject player;
    Vector3 offset;
    float xRotation = 0f;
    private GroundMovement script;

    // Start is called before the first frame update
    void Start()
    {

        offset = transform.position - player.transform.position;
        Cursor.lockState = CursorLockMode.Locked;
        script = player.GetComponent<GroundMovement>();
    }

    // Update is called once per frame
    void Update()
    {
        transform.position = player.transform.position + offset;
        MouseMovement();

        RaycastHit raycastHit;

        if (Input.GetAxis("Fire1") > 0.1f)
        {

            if (Physics.Raycast(GetComponent<Camera>().ScreenPointToRay(Input.mousePosition), out raycastHit))
            {
                if (raycastHit.transform.gameObject.CompareTag("Washing Machine"))
                {
                    var hitobj = raycastHit.transform.gameObject;
                    hitobj.GetComponent<SpriteRenderer>().color = new Color(1f, 0f, 0f);
                    script.IsMovementActive = false;
                }

                if (raycastHit.transform.gameObject.CompareTag("Boiler"))
                {
                    //TODO
                }
            }
        }
    }

    void MouseMovement()
    {
        if (script.IsMovementActive == false) return;

        float mouseX = Input.GetAxis("Mouse X") * mouseSensitivity * Time.deltaTime;
        float mouseY = Input.GetAxis("Mouse Y") * mouseSensitivity * Time.deltaTime;

        xRotation -= mouseY;
        xRotation = Mathf.Clamp(xRotation, -90f, 90f);

        transform.localRotation = Quaternion.Euler(xRotation, 0f, 0f);
        player.transform.Rotate(Vector3.up * mouseX);
    }
}