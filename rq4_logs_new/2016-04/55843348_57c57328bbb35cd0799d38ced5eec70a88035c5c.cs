using UnityEngine;
using System.Collections;
using Vuforia;

public class FireEventScript : MonoBehaviour {
    public float timeBetweenBullets = 0.15f;

    bool singleTab;
    float timer;
    float speed;
    RaycastHit touchHit;
    Ray touchRay;
    int shootableMask;//맞는 유닛인지 확인
    LineRenderer gunLine;//총알 효과
    AudioSource gunAudio;//총소리
    float effectsDisplayTime = 0.2f;
    Vector3 shootTarget;//쏠 위치
    Vector3 shootDir;//쏘는 방향
    
    void Awake()
    {
        shootableMask = LayerMask.GetMask("Shootable");
        gunLine = GetComponent<LineRenderer>();
        gunAudio = GetComponent<AudioSource>();
    }

    void Start()
    {
        singleTab = true;
    }
	
    // Update is called once per frame
    void Update()
    {
        timer += Time.deltaTime;
     if((Input.GetTouch(0).phase == TouchPhase.Began) && timer >= timeBetweenBullets && singleTab)
        {
            Shoot();
        }
        if(timer >= timeBetweenBullets * effectsDisplayTime)
        {
            DisableEffects();
            singleTab = true;
        }
    }

    public void DisableEffects()
    {
        gunLine.enabled = false;
    }

    void Shoot()
    {
        timer = 0f;
        singleTab = false;

        gunAudio.Play();

        gunLine.enabled = true;
        gunLine.SetPosition(0, transform.position);

        GameObject cm = VuforiaManager.Instance.ARCameraTransform.gameObject;
        Camera[] cam = cm.GetComponentsInChildren<Camera>();
        touchRay = cam[0].ScreenPointToRay(Input.mousePosition);
        

        touchRay.origin = transform.position;


        if (Physics.Raycast(touchRay, out touchHit))
        {
            gunLine.SetPosition(1, touchHit.point);
            //takedamage ---> 맞았을때 효과를 추가한다.
        }
        else
            gunAudio.Play();
    }

}