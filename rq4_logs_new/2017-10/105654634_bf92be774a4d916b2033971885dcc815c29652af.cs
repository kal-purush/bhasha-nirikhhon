using UnityEngine;

public class VRMouseEmulatorBehavior : MonoBehaviour
{
    public bool IsDevComponent;

    public Transform Target;

	public bool emulationActive = true;

	public enum RotationAxes { MouseXAndY = 0, MouseX = 1, MouseY = 2 }
	public RotationAxes axes = RotationAxes.MouseXAndY;
	
	public float sensitivity = 5F;
		
	private float minimumY = -90F;
	private float maximumY = 90F;
	
	float rotationY = 0F;

    void Awake()
    {
        Transform t = transform;
        string heirarchy = t.name;
        while (t.parent!= null)
        {
            heirarchy = t.parent.name + ", " + heirarchy;
            t = t.parent;
        }
        if (IsDevComponent)
        {
#if UNITY_EDITOR
            VRMouseEmulatorBehavior[] vrs = Object.FindObjectsOfType<VRMouseEmulatorBehavior>();
            if (vrs.Length > 1)
            {
                enabled = false;
                //Debug.Log("[" + heirarchy+ "] is DISABLED");
            }
            else
            {
                //Debug.Log("[" + heirarchy + "] is ENABLED");
            }
#else
            UnityEngine.Object.Destroy(this);
            //Debug.Log("[" + heirarchy + "] has been destroyed");
#endif
        }
        else
        {
            //Debug.Log("[" + heirarchy + "] is not a dev component");
        }
    }

    void Start()
    {

    }

	void Update ()
    {
		if (emulationActive && Input.GetMouseButton(0) && Target != null)
		{
			if (axes == RotationAxes.MouseXAndY)
			{
                float rotationX = Target.localEulerAngles.y + Input.GetAxis("Mouse X") * sensitivity;
				rotationY += Input.GetAxis("Mouse Y") * sensitivity;
				rotationY = Mathf.Clamp (rotationY, minimumY, maximumY);
                Target.localEulerAngles = new Vector3(-rotationY, rotationX, 0);
			}
			else if (axes == RotationAxes.MouseX)
			{
                Target.Rotate(0,Input.GetAxis("Mouse X") * sensitivity,0);
			}
			else
			{
				rotationY += Input.GetAxis("Mouse Y") * sensitivity;
				rotationY = Mathf.Clamp (rotationY, minimumY, maximumY);
                Target.localEulerAngles = new Vector3(-rotationY, Target.localEulerAngles.y, 0);
			}
		}
	}
}