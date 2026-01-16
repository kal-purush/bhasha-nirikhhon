using UnityEngine;
using System.Collections;

public class CameraManager : MonoBehaviour {


    public Vector3 newTarget;
	public float newAngle;
	public float DefaultZoom;
	public float MoveSpeed;
	public float RotateSpeed;
    public Transform Up;
	public Transform Cam;
    public float MinZoom=-4;
    private float newZoom;
    private float currentAngle;

    public float NewZoom
    {
        get
        {
            return newZoom;
        }

        set
        {
            newZoom = Mathf.Min(MinZoom,value);
        }
    }

    void Start(){
		currentAngle = 0;
		DefaultZoom = 10;
		NewZoom = DefaultZoom * -1;
        Cam.localPosition = Vector3.forward * NewZoom;
	}

	public void SlideToPosition(Vector3 target){
		newTarget = target;
	}

	public void JumpToPosition(Vector3 target){
		newTarget = target;
		transform.position = target;
	}

	public void SlideToRotation(float angle){
		newAngle = angle;
	}


	// Update is called once per frame
	void Update () {
		transform.position = Vector3.Lerp (transform.position, newTarget, MoveSpeed * Time.deltaTime);
		currentAngle = Mathf.Lerp (currentAngle, newAngle, RotateSpeed * Time.deltaTime);
		transform.localEulerAngles = new Vector3 (0,currentAngle,0);
		NewZoom += Input.GetAxis ("Mouse ScrollWheel") * 10;
        Cam.localPosition = new Vector3 (0,0,Mathf.Lerp(Cam.localPosition.z, NewZoom, 4 * Time.deltaTime));
        SlideToPosition(transform.position+ Time.deltaTime*(Vector3.forward*Input.GetAxis("Vertical") * MoveSpeed+ Vector3.right * Input.GetAxis("Horizontal")* MoveSpeed)*-newZoom);
      //  print(Input.mousePosition);
	}
}