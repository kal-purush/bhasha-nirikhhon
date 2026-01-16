using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class FootprintController : MonoBehaviour
{

    private Rigidbody rbody;
    public float rayDist;
    public Material particleMat;

    private ParticleSystem partSystem;
    
    // Start is called before the first frame update
    void Start()
    {
        rbody = GetComponent<Rigidbody>();
        GameObject printController = new GameObject("printController");
        partSystem = printController.AddComponent<ParticleSystem>();
        printController.GetComponent<ParticleSystemRenderer>().material = particleMat;
        printController.GetComponent<ParticleSystemRenderer>().alignment = ParticleSystemRenderSpace.World;
        ParticleSystem.EmissionModule emitMod = partSystem.emission;
        emitMod.rateOverTime = 0f;
        ParticleSystem.MainModule mainMod = partSystem.main;
        mainMod.startLifetime = 1000000;
        mainMod.maxParticles = 100000;
        mainMod.startSpeed = 0f;
        mainMod.startRotation3D = true;
        mainMod.startRotationX = -Mathf.PI/2;
        partSystem.Play();
        StartCoroutine(MakeFootPrints());
    }

    IEnumerator MakeFootPrints()
    {
        while (true)
        {
            
        
            RayStuff myCast = CastRay(rayDist);

            if (myCast.didHit && myCast.rayHit.collider.CompareTag("Loom"))
            {
                if (Input.GetAxis("Horizontal") != 0 || Input.GetAxis("Vertical") != 0)
                {
                    EmitOverride(myCast.rayHit.point + new Vector3(0f, 0.1f, 0f));
                }
            }
            
            yield return new WaitForSeconds(0.45f);
        }
        
    }


    void EmitOverride(Vector3 pos)
    {
        ParticleSystem.EmitParams myParams = new ParticleSystem.EmitParams();
        myParams.position = pos;
        //myParams.rotation3D = Vector3.up;
        myParams.startSize = 0.5f;
        partSystem.Emit(myParams, 1);
        
        Debug.Log("Emitting");
    }

    RayStuff CastRay(float dist)
    {
        RayStuff myRayCastInfo = new RayStuff();
        
        Ray myRay = new Ray(transform.position, Vector3.down);

        myRayCastInfo.didHit = Physics.Raycast(myRay.origin, myRay.direction, out myRayCastInfo.rayHit, dist);

        return myRayCastInfo;
    }
}


class RayStuff
{
    public RaycastHit rayHit;
    public bool didHit;

    public RayStuff()
    {
        rayHit = new RaycastHit();
        didHit = false;
    }

}