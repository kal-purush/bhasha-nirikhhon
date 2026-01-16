using UnityEngine;
using System.Collections;


public class DebugCam : MonoBehaviour
{

    public float mainSpeed = 100.0f; //regular speed
    public float shiftAdd = 250.0f; //multiplied by how long shift is held.  Basically running
    public float maxShift = 1000.0f; //Maximum speed when holdin gshift
    public float camSens = 0.25f; //How sensitive it with mouse
    private Vector3 lastMouse = new Vector3(255, 255, 255); //kind of in the middle of the screen, rather than at the top (play)
    private float totalRun = 1.0f;

    public float Sensitivity;
    public bool YInvert;
    //Debug var
    public bool Show_Stats;
    public bool Show_FPS;
    public bool Show_Tris;
    public bool Show_Verts;
    public float updateInterval = 0.5F;
    public static int verts;
    public static int tris;
    public float fps;

    private float fpsaAccum = 0;
    private int frames = 0;
    private float timeleft;


    private void Start()
    {
        //Hide the cursor
        Cursor.visible = false;
        timeleft = updateInterval;
    }

    void Update()
    {
        float rotationY = Input.GetAxis("Mouse X") * Sensitivity;
        float rotationX = 0;
        if (YInvert)
        {
            rotationX = -Input.GetAxis("Mouse Y") * Sensitivity;
        }
        else
        {
            rotationX = Input.GetAxis("Mouse Y") * Sensitivity;
        }

        transform.Rotate(new Vector3(rotationX, rotationY));
        Vector3 ea = transform.eulerAngles;
        transform.eulerAngles = new Vector3(ea.x, ea.y, 0);

        //camera angle done.  

        //Keyboard commands
        float f = 0.0f;
        Vector3 p = GetBaseInput();
        if (Input.GetKey(KeyCode.LeftShift))
        {
            totalRun += Time.deltaTime;
            p = p * totalRun * shiftAdd;
            p.x = Mathf.Clamp(p.x, -maxShift, maxShift);
            p.y = Mathf.Clamp(p.y, -maxShift, maxShift);
            p.z = Mathf.Clamp(p.z, -maxShift, maxShift);
        }
        else
        {
            totalRun = Mathf.Clamp(totalRun * 0.5f, 1f, 1000f);
            p = p * mainSpeed;
        }

        p = p * Time.deltaTime;
        Vector3 newPosition = transform.position;
        if (Input.GetKey(KeyCode.Space))
        { //If player wants to move on X and Z axis only
            transform.Translate(p);
            newPosition.x = transform.position.x;
            newPosition.z = transform.position.z;
            transform.position = newPosition;
        }
        else
        {
            transform.Translate(p);
        }
        //WireFrame
        if (Input.GetKeyDown(KeyCode.O))
        {
            if (!GL.wireframe)
            {
                Debug.Log("WireFrame Activated.");
                GL.wireframe = true;
            }
            else
            {
                Debug.Log("WireFrame Deactivated.");
                GL.wireframe = false;
            }
        }
        //ShowTriangles
        if (Input.GetKeyDown(KeyCode.T))
        {
            if (!GL.wireframe)
            {
                Debug.Log("WireFrame Activated.");
                GL.wireframe = true;
            }
            else
            {
                Debug.Log("WireFrame Deactivated.");
                GL.wireframe = false;
            }
        }
        //Updating
        timeleft -= Time.deltaTime;
        fpsaAccum += Time.timeScale / Time.deltaTime;
        ++frames;

        if (timeleft <= 0.0)
        {
            // display two fractional digits (f2 format)
            fps = fpsaAccum / frames;
            string format = System.String.Format("{0:F2} FPS", fps);
            //  DebugConsole.Log(format,level);
            timeleft = updateInterval;
            fpsaAccum = 0.0F;
            frames = 0;
            GetObjectStats();
        }

    }
    void GetObjectStats()
    {
        verts = 0;
        tris = 0;
        GameObject[] ob = FindObjectsOfType(typeof(GameObject)) as GameObject[];
        foreach (GameObject obj in ob)
        {
            GetObjectStats(obj);
        }
    }
    void GetObjectStats(GameObject obj)
    {
        Component[] filters;
        filters = obj.GetComponentsInChildren<MeshFilter>();
        foreach (MeshFilter f in filters)
        {
            tris += f.sharedMesh.triangles.Length / 3;
            verts += f.sharedMesh.vertexCount;
        }
    }
    void OnGUI()
    {
        ShowDegubModeStats();
    }
    private Vector3 GetBaseInput()
    { //returns the basic values, if it's 0 than it's not active.
        Vector3 p_Velocity = new Vector3();
        if (Input.GetKey(KeyCode.W))
        {
            p_Velocity += new Vector3(0, 0, 1);
        }
        if (Input.GetKey(KeyCode.S))
        {
            p_Velocity += new Vector3(0, 0, -1);
        }
        if (Input.GetKey(KeyCode.A))
        {
            p_Velocity += new Vector3(-1, 0, 0);
        }
        if (Input.GetKey(KeyCode.D))
        {
            p_Velocity += new Vector3(1, 0, 0);
        }
        return p_Velocity;
    }
    void ShowDegubModeStats()
    {
        GUILayout.BeginArea(new Rect(0, 0, 100, 100));
        if (Show_FPS)
        {
            string fpsdisplay = fps.ToString("#,##0 fps");
            GUILayout.Label(fpsdisplay);
        }
        if (Show_Tris)
        {
            string trisdisplay = tris.ToString("#,##0 tris");
            GUILayout.Label(trisdisplay);
        }
        if (Show_Verts)
        {
            string vertsdisplay = verts.ToString("#,##0 verts");
            GUILayout.Label(vertsdisplay);
        }
        GUILayout.EndArea();
    }
}