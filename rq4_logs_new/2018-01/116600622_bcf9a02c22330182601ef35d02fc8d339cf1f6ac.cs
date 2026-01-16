using UnityEngine;
using UnityEngine.Networking;

public class Gun : NetworkBehaviour
{
    [SerializeField]
    GunType data;

    private void Awake()
    {
        gunName = data.gunTypeName;
        bulletPrefab = data.bulletPrefab;
        maxAmmo = data.startMaxAmmo;
        reloadRate = data.reloadRate;

        fireRate = data.fireRate;
        shotSpeed = data.shotSpeed;
        spread = data.spread;
        splash = data.splash;
        splashCount = data.splashCount;

        damage = data.damage;
    }

    public string gunName;
    [SerializeField]
    GameObject bulletPrefab;
    float reloadRate;
    [SyncVar]
    public float fireRate;
    [SyncVar]
    public float shotSpeed;
    [SyncVar]
    public float spread;
    bool splash;
    int splashCount;

    [SyncVar]
    public int damage;
    [SyncVar]
    public int ammo;
    [SyncVar]
    public int maxAmmo;
    
    Transform muzzle;
    
    float subtractOffset;
    float fireTimer;

    [SyncVar(hook = "OnRenderer")]
    public bool isRendererEnabled = true;

    public void OnRenderer(bool valueToChangeTo)
    {
        isRendererEnabled = valueToChangeTo;
        GetComponent<SpriteRenderer>().enabled = isRendererEnabled;
    }

    /*[SyncVar(hook = "SwapParent")]
    public Transform gunParent = null;

    public void SwapParent(Transform parentToSet)
    {
        gunParent = parentToSet;
        transform.SetParent(gunParent);
    }*/

    public void Fire(string team)
    {
        if (ammo > 0)
        {
            if (Time.time > fireTimer)
            {
                if (splash) //shotgun or similar
                {
                    for (int i = 0; i < splashCount; i++)
                    {
                        switch (splashCount)
                        {
                            case 3:
                                subtractOffset = .5f;
                                break;
                            case 4:
                                subtractOffset = 1f;
                                break;
                            case 5:
                                subtractOffset = 2f;
                                break;
                        }

                        GameObject bullet = Instantiate(bulletPrefab, muzzle.position, muzzle.rotation);
                        bullet.transform.Rotate(new Vector3(0f, 0f, (i - subtractOffset)));
                        bullet.GetComponent<Rigidbody2D>().velocity = bullet.transform.right * shotSpeed;
                        bullet.GetComponent<Missile>().damage = damage;
                        bullet.GetComponent<Missile>().team = team;
                        NetworkServer.Spawn(bullet);
                    }
                }
                else //one shot, one bullet
                {
                    GameObject bullet = Instantiate(bulletPrefab, muzzle.position, muzzle.rotation);
                    bullet.transform.Rotate(new Vector3(0f, 0f, (UnityEngine.Random.Range(-spread, spread))));
                    bullet.GetComponent<Rigidbody2D>().velocity = bullet.transform.right * shotSpeed;
                    bullet.GetComponent<Missile>().damage = damage;
                    bullet.GetComponent<Missile>().team = team;
                    NetworkServer.Spawn(bullet);
                }
                fireTimer = Time.time + fireRate;
                ammo--;
            }
        }
    }

    public void Reload()
    {
        ammo = maxAmmo;
    }

    public void SetMuzzle(Transform muzzleLoc)
    {
        muzzle = muzzleLoc;
    }

    private void Update()
    {
        if (!isLocalPlayer)
        {
            return;
        }

        transform.position = transform.parent.position;
    }
}