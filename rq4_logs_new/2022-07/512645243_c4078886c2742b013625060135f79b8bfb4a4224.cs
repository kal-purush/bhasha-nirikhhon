using UnityEngine;
using System.Collections;

//This was made in 2015 don't blame me 
public class BasicFPP : MonoBehaviour
{
    //Clases
    //Mouse look class
    [System.Serializable]
    public class ML
    {
        public GameObject MainBody;
        public float LookSpeed = 1;
        public float HorizontalLerpSpeed = 1;
        public float VerticalLerpSpeed = 1;
        public float VerticalAxisLimit = 90;   // Vertical axis clamp
        //Variables Internas
        public float MouseX; // Current Mouse input in X
        public float MouseY;//  Current Mouse input in Y
        public float MouseXlerp;
        public float MouseYlerp;
    };
    
    //Movement control Class
    [System.Serializable]
    public class MVC
    {
        public float VerticalSpeed = 1;   //foward and backward  speed
        public float HorizontalSpeed = 1; //Left and roght speed;
        //TODO: IMPLEMENTAR CAPACIDAD PARA SALTAR >:V
        // public float JumpForce = 4; 
        public float GravityForce = -9.81f;
        //Sound
        public AudioSource AudioPoint;
        public AudioClip FootStep;
        public float StepRatio = 0.3f;
        public float MinSpeedToPlay = 0.4f;
        //Varaibles internas
        public float InputX;
        public float InpuY;
    };

    //Variables del script
    public bool IsOutOfAnyVehicle = true;
    public ML MouseLook;
    public MVC MovementControl;
    //Variables internas >:V
    float Ntime;// for foot steps ratio effect
   
    //GUN LOGIC
    public int gunDamage = 1;
    public float fireRate = 0.25f;                                     
    public float weaponRange = 50f;                                 
    public float hitForce = 100f;                                    
    public Transform gunEnd;
    public Camera fpsCam;
    private WaitForSeconds shotDuration = new WaitForSeconds(0.07f);    
    private AudioSource gunAudio;                            
    public GameObject muzzleFlash;
    public GameObject impactPoint;

    private float nextFire;                                         
    public delegate void BulletHit(GameObject target);
    public event BulletHit OnBulletHit;
    private Vector3 hitPos;

    public void OnDrawGizmos()
    {
            Gizmos.color = Color.red;
            Gizmos.DrawSphere(hitPos,1);
    }

    void GunLogic()
    {
        // Check if the player has pressed the fire button and if enough time has elapsed since they last fired
        if (Input.GetButtonDown("Fire1") && Time.time > nextFire)
        {
            // Update the time when our player can fire next
            nextFire = Time.time + fireRate;
            // Start our ShotEffect coroutine to turn our laser line on and off
            StartCoroutine(ShotEffect());
            RaycastHit hit;
            //laserLine.SetPosition(0, fpsCam.transform.position);

            // Check if our raycast has hit anything
            if (Physics.Raycast(fpsCam.transform.position, fpsCam.transform.forward, out hit, weaponRange))
            {
                // Set the end position for our laser line 
                hitPos = hit.point;
                impactPoint.transform.position = hit.point;
                //if (OnBulletHit != null)
                    //OnBulletHit(hit.transform.gameObject);
            }
            else
            {
                // If we did not hit anything, set the end of the line to a position directly in front of the camera at the distance of weaponRange
                //laserLine.SetPosition(1, gunEnd.position + (fpsCam.transform.forward * weaponRange));
            }
        }
    }

    private IEnumerator ShotEffect()
    {
        // Play the shooting sound effect
        //gunAudio.Play();

        // Turn on our line renderer
        muzzleFlash.SetActive(true);
        impactPoint.SetActive(true);
        //Wait for .07 seconds
        yield return shotDuration;

        // Deactivate our line renderer after waiting
        muzzleFlash.SetActive(false);
        //impactPoint.SetActive(false);
    }

    void FPPinput()//Gets all input of the keyboard and mouse 
    {
        //MOUSE 
        MouseLook.MouseX += Input.GetAxis("Mouse X") * MouseLook.LookSpeed;
        MouseLook.MouseY -= Input.GetAxis("Mouse Y") * MouseLook.LookSpeed;
        //Keyboard
        MovementControl.InputX = Input.GetAxis("Horizontal") * MovementControl.HorizontalSpeed;
        MovementControl.InpuY = Input.GetAxis("Vertical") * MovementControl.HorizontalSpeed;
    }
    
    void OnGround()
    {
        //Mouse look
        //el transform actual deberia ser la camara
        //Creaamos las interpolaciones necesarias pero primero hacemos el clamp
        MouseLook.MouseY = Mathf.Clamp(MouseLook.MouseY, -MouseLook.VerticalAxisLimit, MouseLook.VerticalAxisLimit);
        MouseLook.MouseXlerp = Mathf.Lerp(MouseLook.MouseXlerp, MouseLook.MouseX, MouseLook.HorizontalLerpSpeed * Time.deltaTime);
        MouseLook.MouseYlerp = Mathf.Lerp(MouseLook.MouseYlerp, MouseLook.MouseY, MouseLook.VerticalLerpSpeed * Time.deltaTime);
      
        //asginamos valores
        transform.localRotation = Quaternion.Euler(new Vector3(MouseLook.MouseYlerp, 0, 0));//Camra
        MouseLook.MainBody.transform.rotation = Quaternion.Euler(new Vector3(0, MouseLook.MouseXlerp, 0));//body
    
        //Movement Control
        Vector3 MovementDirection = new Vector3(MovementControl.InputX * MovementControl.HorizontalSpeed, 0, MovementControl.InpuY * MovementControl.VerticalSpeed);
        Vector3 Dir = MouseLook.MainBody.transform.TransformDirection(MovementDirection);
        MouseLook.MainBody.GetComponent<CharacterController>().SimpleMove(Dir);

    }

    //Output methods
    void Start()
    {
    }
    public bool lockState = true;

    void Update()
    {
        if(Input.GetKeyDown(KeyCode.F))
            lockState = !lockState;
        
        if (lockState)
            Cursor.lockState = CursorLockMode.Locked;
        else
            Cursor.lockState = CursorLockMode.None;

        FPPinput();
        OnGround();
        GunLogic();     
    }
}