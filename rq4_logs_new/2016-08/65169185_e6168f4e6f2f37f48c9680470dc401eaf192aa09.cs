using UnityEngine;
using System.Collections.Generic;

public class RangeFinder : MonoBehaviour
{
    public bool DEBUG = true;

    public GameObject crosshair;
    private Camera mainCamera;
    public float maxDistance = 10f;
    private float targetDistance;
    private RaycastHit hit;
    public float facingTolerance = -0.85f;

    private GameObject[] allTargets;
    private RaycastHit visibilityRay;
    private Vector3 visibilityVector;

    private float zoomAmount = 60f;
    private float maxZoomIn = 10f;
    private float maxZoomOut = 60f;
    public float zoomSpeed = 20f;

    private Collider target;
    private bool facing;
    private float facingProduct;
    private int numberOfTargetsInPhoto;

    [HideInInspector] public SessionPictureInfo sessionPictureInfo;
    [HideInInspector] public ListVisibleTargets listVisibleTargets;
    [HideInInspector] public SessionPhotos sessionPhotos;
    public List<SessionPictureInfo.PictureInfo> pictureInfo;
    private List<Collider> visibleTargets;

    [HideInInspector] public Renderer preview;
    [HideInInspector] public Texture2D tex;
    [HideInInspector] public bool grabScreenshot;


    void Awake()
    {
        sessionPictureInfo = GetComponent<SessionPictureInfo>();
        pictureInfo = new List<SessionPictureInfo.PictureInfo>();
        //preview = GetComponent<Renderer>();

        listVisibleTargets = GetComponent<ListVisibleTargets>();
        sessionPhotos = GetComponent<SessionPhotos>();
    }


    void Start()
    {
        mainCamera = GetComponentInChildren<Camera>();
    }


    void GetVisibilityOfTargets()
    {
        numberOfTargetsInPhoto = 0;
        allTargets = GameObject.FindGameObjectsWithTag("Target");
        visibleTargets = new List<Collider>();

        for (int i = 0; i < allTargets.Length; i++)
        {
            visibilityVector = allTargets[i].transform.position - crosshair.transform.position;
            Physics.Raycast(crosshair.transform.position, visibilityVector, out visibilityRay, Mathf.Infinity);
            if (DEBUG) { Debug.DrawRay(crosshair.transform.position, visibilityVector, Color.red, Mathf.Infinity); }

            Vector3 viewPos = mainCamera.WorldToViewportPoint(allTargets[i].transform.position);
            if (viewPos.x <= 0 || viewPos.x >= 1 || viewPos.y <= 0 || viewPos.y >= 1 || viewPos.z <= 0)
            {
                //Targets are disregarded if they are off-screen
                if (DEBUG) { Debug.Log("Waypoint " + i + " -- target is OFF SCREEN -- " + viewPos); }
            }
            else
            {
                if (visibilityRay.collider.tag != "Target")
                {
                    //Targets are disregarded if they are on-screen but blocked
                    if (DEBUG) { Debug.Log(allTargets[i] + " is BLOCKED by " + visibilityRay.collider.name); }
                }
                else
                {
                    //Targets are added to a list
                    numberOfTargetsInPhoto += 1;
                    visibleTargets.Add(visibilityRay.collider);
                    if (DEBUG) { Debug.Log(allTargets[i] + " is VISIBLE -- " + viewPos); }
                }
            }
        }
        if (allTargets.Length == 0)
        {
            if (DEBUG) { Debug.Log("There are no targets in the scene."); }
        }
    }


    void GetTarget()
    {
        target = null;
        if (Physics.Raycast(crosshair.transform.position, crosshair.transform.forward, out hit, maxDistance))
        {
            target = hit.collider;

            //Cursor hits that are not valid targets are disregarded
            if (target.tag != "Target" || target == null)
            {
                target = null;
            }
            else
            {
                targetDistance = hit.distance;
                if (DEBUG) { Debug.Log(target.name + " is " + targetDistance.ToString("F1") + " away "); }
                //TO DO: GET SPECIAL POSE, IF ANY
            }
        }
    }


    void GetTargetFill()
    {
        
    }


    void GetOrientationOfTarget()
    {
        //Targets facing you will accrue more points
        facing = false;
        if (target == null)
        {
            if (DEBUG) { Debug.Log("You have no target!"); }
        }
        else
        {
            facingProduct = Vector3.Dot(crosshair.transform.forward, target.transform.forward);

            if (facingProduct >= -1 && facingProduct <= facingTolerance)
            {
                facing = true;
                if (DEBUG) { Debug.Log(target.name + " is facing you."); }
            }
            else
            {
                facing = false;
                if (DEBUG) { Debug.Log(target.name + " isn't facing you"); }
            }
        }
    }


    void TakeSnapshot()
    {
        tex = new Texture2D(Screen.width, Screen.height, TextureFormat.RGB24, false);
        grabScreenshot = true;
    }


    void CollectPictureInfo()
    {
        GetVisibilityOfTargets();
        GetTarget();
        //GetTargetFill();
        GetOrientationOfTarget();
        TakeSnapshot();
    }


    public void SavePictureInfo()
    {
        pictureInfo.Add(new SessionPictureInfo.PictureInfo(target, facing, numberOfTargetsInPhoto, visibleTargets, tex));
    }


    void Update()
    {
        zoomAmount -= Input.GetAxis("Mouse ScrollWheel") * zoomSpeed;
        zoomAmount = Mathf.Clamp(zoomAmount, maxZoomIn, maxZoomOut);
        mainCamera.fieldOfView = zoomAmount;

        if (Input.GetMouseButtonDown(0))
        {
            CollectPictureInfo();
            SavePictureInfo();
        }

        //DEBUG
        if (Input.GetKeyDown(KeyCode.Delete))
        {
            pictureInfo.Clear();
            visibleTargets.Clear();
        }
    }
}