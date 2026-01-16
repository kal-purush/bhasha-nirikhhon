using UnityEngine;

public class BulletEmitter : MonoBehaviour
{

    public Transform bulletEmitter;                 //Get Bullet emitters Pos+Rot
    public GameObject bulletGFX;                    //Get Bullet graphic
    public float bulletSpeed = 100.0f;              //Foward force of bullet
    public float fireRate = 0.25f;                  //Speed between shots
    public int innerPelletCount = 4;                //Pellets in the centre
    public int outerPelletCount = 6;                //Pellets in the outer spread
    public float shotSpread = 5.0f;                 //Shotgun Spread


    float nextShot;                                 //ready to shoot


    // For bullet spread
    private Quaternion pelletRot;

    [HideInInspector]
    public bool shotReady;


    void Start()
    {
        nextShot = fireRate;
    }

    void Update()
    {
        OnFire();
    }

    /* Firerate and shot spawn */

    private void OnFire()
    {
        if (nextShot >= fireRate)
            shotReady = true;
        else
        {
            shotReady = false;
        }

        // Triggers when the gun is fired
        if (Input.GetMouseButtonDown(0) && shotReady)
        {
            // Play sound(s)
            FindObjectOfType<AudioManager>().Play("shotgunBass");

            // Begin Cooldown
            nextShot = 0;

            // Spawn pellets
            for (int i = 0; i < innerPelletCount; i++)
            {
                pelletRot = Random.rotation;

                GameObject activePellet = Instantiate(bulletGFX, bulletEmitter.position, bulletEmitter.rotation) as GameObject;
                activePellet.transform.rotation = Quaternion.RotateTowards(activePellet.transform.rotation, pelletRot, shotSpread/4);

                Rigidbody activeBulletRB = activePellet.GetComponent<Rigidbody>();
                activeBulletRB.velocity = activePellet.transform.forward * bulletSpeed;
            }
            for (int i = 0; i < outerPelletCount; i++)
            {
                pelletRot = Random.rotation;

                GameObject activePellet = Instantiate(bulletGFX, bulletEmitter.position, bulletEmitter.rotation) as GameObject;
                activePellet.transform.rotation = Quaternion.RotateTowards(activePellet.transform.rotation, pelletRot, shotSpread);

                Rigidbody activeBulletRB = activePellet.GetComponent<Rigidbody>();
                activeBulletRB.velocity = activePellet.transform.forward * bulletSpeed;
            }

            // Spawn Pellet Glow lights
            /*  Light per bullet looks good for now
             *  
                for (int i = 0; i < lightCount; i++) {
                lightRot = Random.rotation;

                GameObject tmpLight = Instantiate(PelletLight, bulletEmitter.position, bulletEmitter.rotation) as GameObject;
                tmpLight.transform.rotation = Quaternion.RotateTowards(tmpLight.transform.rotation, lightRot, lightSpread);

                Rigidbody tmpLightRB = tmpLight.GetComponent<Rigidbody>();
                tmpLightRB.velocity = tmpLight.transform.forward * bulletSpeed;
                Destroy(tmpLight, bulletDespawn * Time.deltaTime);
            }*/



        }

        // Charge next shot
        nextShot += 1.0f * Time.deltaTime;

        // Stop charging when fire is ready      
        if (nextShot > fireRate)
        nextShot = fireRate;
    }


}