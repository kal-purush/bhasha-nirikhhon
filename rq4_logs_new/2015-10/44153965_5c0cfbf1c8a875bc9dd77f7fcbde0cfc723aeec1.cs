using UnityEngine;
using System.Collections;

public class CameraControl : MonoBehaviour {

	public bool enabledMovement = true;
	public float mapLength = 40;
	public float digLength = 0;
	public float speed = 0.25f;
    
    private int horizontal;
    private int vertical;
    private Vector2 touchOrigin;

	void Start()
	{
		digLength = Mathf.Sqrt(mapLength * mapLength + mapLength * mapLength);
	}

	void Update () {

			if(enabledMovement)
			{
                horizontal = (int)Input.GetAxisRaw("Horizontal");
                vertical = (int)Input.GetAxisRaw("Vertical");

                if (Input.touchCount > 0 && Input.GetTouch(0).phase == TouchPhase.Moved)
                {
                    Vector2 touchDeltaPosition = Input.GetTouch(0).deltaPosition;
                    horizontal = -(int)(touchDeltaPosition.x /40);
                    vertical = -(int)(touchDeltaPosition.y / 40);
                }

                if (transform.position.x > 20 || transform.position.x < -20)
                    horizontal = 0;
				transform.Translate(new Vector3(horizontal,0,vertical));

                if (Input.touchCount == 2)
                {
                    Touch touchZero = Input.GetTouch(0);
                    Touch touchOne = Input.GetTouch(1);

                    Vector2 touchZeroPrevPos = touchZero.position - touchZero.deltaPosition;
                    Vector2 touchOnePrevPos = touchOne.position - touchOne.deltaPosition;

                    float prevTouchDeltaMag = (touchZeroPrevPos - touchOnePrevPos).magnitude;
                    float touchDeltaMag = (touchZero.position - touchOne.position).magnitude;

                    float deltaMagnitudeDiff = prevTouchDeltaMag - touchDeltaMag;

                    transform.FindChild("Main Camera").GetComponent<Camera>().orthographicSize += deltaMagnitudeDiff * .02f;
                    transform.FindChild("Main Camera").GetComponent<Camera>().orthographicSize = Mathf.Max(transform.FindChild("Main Camera").GetComponent<Camera>().orthographicSize, 0.1f);
                }

                if (Input.GetAxis("Mouse ScrollWheel") < 0)
                    if (transform.FindChild("Main Camera").GetComponent<Camera>().orthographicSize <= 10)
                        transform.FindChild("Main Camera").GetComponent<Camera>().orthographicSize += 0.5f;

                if (Input.GetAxis("Mouse ScrollWheel") > 0)
                    if (transform.FindChild("Main Camera").GetComponent<Camera>().orthographicSize >= 0)
                        transform.FindChild("Main Camera").GetComponent<Camera>().orthographicSize -= 0.5f;	

			}
			
		}
	}