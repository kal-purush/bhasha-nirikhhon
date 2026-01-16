using System;
using UnityEngine;
using UnityEngine.Networking;

public class FighterController : NetworkBehaviour
{
    GameObject pickup;
    bool cooldown;

    [SerializeField]
    GunType equippedGun;

    [SyncVar]
    public int ammo;
    [SyncVar]
    public int maxAmmo;
    public Transform muzzle;

    float subtractOffset;

    public GunType EquippedGun
    {
        get
        {
            return equippedGun;
        }
        set
        {
            this.equippedGun = value;
            ammo = 0;
            maxAmmo = equippedGun.startMaxAmmo;
        }
    }

    public FighterType ft;

    public delegate void TakeDamageDelegate(int side, int damage);
    public delegate void DieDelegate();
    public delegate void RespawnDelegate();

    [SyncEvent(channel = 1)]
    public event TakeDamageDelegate EventTakeDamage;

    [SyncEvent]
    public event DieDelegate EventDie;

    [SyncEvent]
    public event RespawnDelegate EventRespawn;

    [SyncVar]
    public int health = 100;

    [SyncVar]
    public int energy = 100;

    [SyncVar]
    public bool alive = true;

    [SyncVar]
    public bool sprinting = false;

    [SyncVar]
    public string fighterName;

    [SyncVar]
    public string fighterType;

    [SyncVar]
    public string team;

    float regenTimer;

    Vector2 moveSpeed;

    // for client movement command throftling
    Vector2 oldMoveForce = new Vector2(0, 0);

    // server movement
    [SyncVar]
    Vector2 moving;

    public bool autoMove = false;

    bool firingWeapon;
    bool interacting;
    bool m_focus = true;

    [SyncVar]
    public float upperBodyAngle;

    public Transform upperBody;
    public GameObject explosion;
    public GameObject tracks;
    Rigidbody2D physBody;

    public float trackTimer;

    float sprintTimer;
    float fireWeaponTimer;
    float turrentSendTimer = 0.0f;
    float turrentSendDelay = 0.1f;

    // Use this for initialization
    void Start()
    {

    }

    void OnTriggerStay2D(Collider2D col)
    {
        if (cooldown == false)
        {
            if (col.CompareTag("Item"))
            {
                pickup = col.gameObject;
                Invoke("ResetPickup", 0.3f);
            }
            Invoke("ResetCooldown", 0.5f);
            cooldown = true;
        }
    }

    void ResetPickup()
    {
        pickup = null;
    }

    void ResetCooldown()
    {
        cooldown = false;
    }

    [Command]
    public void CmdInteract()
    {
        if (!fighterActionsAllowed())
            return;

        if (pickup == null)
            return;

        if (pickup.GetComponent<GunPickup>() != null)
        {
            GunType oldGun = EquippedGun;
            if (oldGun != null)
            {
                GameObject newPickup = Instantiate(oldGun.pickupPrefab, transform.position, Quaternion.identity);
                NetworkServer.Spawn(newPickup);
                EquippedGun = pickup.GetComponent<GunPickup>().gun;
                Destroy(pickup);
            }
            else
            {
                EquippedGun = pickup.GetComponent<GunPickup>().gun;
                Destroy(pickup);
            }
        }
    }

    [Server]
    public void InitializeFromFighterType(FighterType newFT)
    {
        ft = newFT;
        fighterType = ft.fighterTypeName;
        health = ft.maxHealth;
        energy = ft.maxEnergy;
        fighterName = Guid.NewGuid().ToString().Substring(0, 8);
        team = "team-" + fighterName;

        GetComponent<SpriteRenderer>().sprite = ft.skinBody;
    }

    public override void OnStartClient()
    {
        if (NetworkServer.active)
            return;

        //TODO
        FighterType found = FighterTypeManager.Lookup(fighterType);
        ft = found;
        GetComponent<SpriteRenderer>().sprite = ft.skinBody;
    }

    void Awake()
    {
        physBody = GetComponent<Rigidbody2D>();
        maxAmmo = equippedGun.startMaxAmmo;
        autoDeathTime = Time.time + UnityEngine.Random.Range(5, 60);
    }

    public void Fire(string team)
    {
        if (equippedGun != null && ammo > 0)
        {
            if (equippedGun.splash) //shotgun or similar
            {
                for (int i = 0; i < equippedGun.splashCount; i++)
                {
                    switch (equippedGun.splashCount)
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

                    GameObject bullet = Instantiate(equippedGun.bulletPrefab, muzzle.position, muzzle.rotation);
                    bullet.transform.Rotate(new Vector3(0f, 0f, (i - subtractOffset)));
                    bullet.GetComponent<Rigidbody2D>().velocity = bullet.transform.right * equippedGun.shotSpeed;
                    bullet.GetComponent<Missile>().damage = equippedGun.damage;
                    bullet.GetComponent<Missile>().team = team;
                    NetworkServer.Spawn(bullet);
                }
            }
            else //one shot, one bullet
            {
                GameObject bullet = Instantiate(equippedGun.bulletPrefab, muzzle.position, muzzle.rotation);
                bullet.transform.Rotate(new Vector3(0f, 0f, (UnityEngine.Random.Range(-equippedGun.spread, equippedGun.spread))));
                bullet.GetComponent<Rigidbody2D>().velocity = bullet.transform.right * equippedGun.shotSpeed;
                bullet.GetComponent<Missile>().damage = equippedGun.damage;
                bullet.GetComponent<Missile>().team = team;
                NetworkServer.Spawn(bullet);
            }

            ammo--;
        }
    }


    [Server]
    public void GotHit(int side, int damage, string team)
    {
        if (team != null && this.team == team)
        {
            return;
        }

        EventTakeDamage(side, damage);
        TakeDamage(damage);
    }


    [Server]
    public void TakeDamage(int amount)
    {
        if (!alive)
            return;

        if (health > amount)
        {
            health -= amount;
        }
        else
        {
            health = 0;
            alive = false;
            physBody.velocity = Vector2.zero;
            // play explosion!
            EventDie();
        }
    }

    bool fighterActionsAllowed()
    {
        return !PlayGame.GetComplete() && alive;
    }

    [Command]
    public void CmdFire()
    {
        Fire(team);
    }

    [Command]
    public void CmdReload()
    {
        Reload();
    }

    [Server]
    public void Reload()
    {
        if (!fighterActionsAllowed())
        {
            return;
        }
        ammo = maxAmmo;
    }

    [Server]
    void UpdateServer()
    {
        HandleServerMovement();
        EnergyRegen();
    }

    [Server]
    void EnergyRegen()
    {
        // energy recharges over time
        if (Time.time > regenTimer)
        {
            if (energy < ft.maxEnergy && !sprinting)
            {
                energy += ft.energyRegen;
            }
            regenTimer = Time.time + 0.1f;
        }
    }

    [Server]
    void HandleServerMovement()
    {
        if (!alive)
        {
            physBody.drag = 5;
            physBody.angularDrag = 5;
            return;
        }

        if (!fighterActionsAllowed())
        {
            return;
        }

        if (sprinting)
        {
            if (sprintTimer == 0)
            {
                sprintTimer = ft.sprintRate;
            }
            if (energy < ft.sprintEnergy)
            {
                sprinting = false;
            }
            else if (Time.time > sprintTimer)
            {
                energy -= ft.sprintEnergy;
                sprintTimer = Time.time + ft.sprintRate;
            }
        }
        // update x movement
        if (moving.x > 0)
        {
            if (!sprinting)
            {
                moveSpeed.x += ft.acceleration;
                if (moveSpeed.x >= ft.topRunSpeed)
                {
                    moveSpeed.x = ft.topRunSpeed;
                }
            }
            else if (sprinting)
            {
                moveSpeed.x += (1.8f * ft.acceleration);
                if (moveSpeed.x >= ft.topSprintSpeed)
                {
                    moveSpeed.x = ft.topSprintSpeed;
                }
            }
        }
        else if (moving.x < 0)
        {
            if (!sprinting)
            {
                moveSpeed.x -= ft.acceleration;
                if (moveSpeed.x <= -ft.topRunSpeed)
                {
                    moveSpeed.x = -ft.topRunSpeed;
                }
            }
            else if (sprinting)
            {
                moveSpeed.x -= (1.8f * ft.acceleration);
                if (moveSpeed.x <= -ft.topSprintSpeed)
                {
                    moveSpeed.x = -ft.topSprintSpeed;
                }
            }
        }
        else
        {
            moveSpeed.x *= 0.18f;
            if (moveSpeed.x < 0.1 && moveSpeed.x > -0.1)
            {
                moveSpeed.x = 0;
            }
        }

        // update y movement
        if (moving.y > 0)
        {
            if (!sprinting)
            {
                moveSpeed.y += ft.acceleration;
                if (moveSpeed.y >= ft.topRunSpeed)
                {
                    moveSpeed.y = ft.topRunSpeed;
                }
            }
            else if (sprinting)
            {
                moveSpeed.y += (1.8f * ft.acceleration);
                if (moveSpeed.y >= ft.topSprintSpeed)
                {
                    moveSpeed.y = ft.topSprintSpeed;
                }
            }
        }
        else if (moving.y < 0)
        {
            if (!sprinting)
            {
                moveSpeed.y -= ft.acceleration;
                if (moveSpeed.y <= -ft.topRunSpeed)
                {
                    moveSpeed.y = -ft.topRunSpeed;
                }
            }
            else if (sprinting)
            {
                moveSpeed.y -= (1.8f * ft.acceleration);
                if (moveSpeed.y <= -ft.topSprintSpeed)
                {
                    moveSpeed.y = -ft.topSprintSpeed;
                }
            }
        }
        else
        {
            moveSpeed.y *= 0.18f;
            if (moveSpeed.y < 0.1 && moveSpeed.y > -0.1)
            {
                moveSpeed.y = 0;
            }
        }

        Vector2 d = new Vector2(moveSpeed.x, moveSpeed.y);
        physBody.velocity = d;

        if (autoMove)
        {
            AutoMove();
        }
    }

    void FireWeapon()
    {
        if (Time.time > fireWeaponTimer)
        {
            fireWeaponTimer = Time.time + EquippedGun.fireRate;
            CmdFire();
        }
    }

    void HandlePlayerMovement()
    {
        float xMoveForce = Input.GetAxis("Horizontal");
        float yMoveForce = Input.GetAxis("Vertical");
        if (xMoveForce > 0) { xMoveForce = 1; }
        if (xMoveForce < 0) { xMoveForce = -1; }
        if (yMoveForce > 0) { yMoveForce = 1; }
        if (yMoveForce < 0) { yMoveForce = -1; }
        Vector2 moveForce = new Vector2(xMoveForce, yMoveForce);
        moveForce = moveForce.normalized;

        if (oldMoveForce != moveForce)
        {
            CmdWalk(moveForce);
            oldMoveForce = moveForce;
        }
    }

    [Server]
    void RotateUpperBody(float angle)
    {
        if (!fighterActionsAllowed())
        {
            return;
        }

        Vector3 rotationVector = new Vector3(0, 0, angle);
        upperBody.transform.rotation = Quaternion.Euler(rotationVector);
        upperBodyAngle = angle;
    }

    [Command]
    public void CmdRotateUpperBody(float angle)
    {
        RotateUpperBody(angle);
    }

    [Server]
    void Walk(Vector2 moving)
    {
        if (PlayGame.GetComplete() || !alive)
        {
            this.moving.x = 0;
            this.moving.y = 0;
            return;
        }

        this.moving = moving;
    }

    [Command]
    public void CmdWalk(Vector2 moving)
    {
        Walk(moving);
    }

    float autoMoveTimer = 0;
    float autoTurret;
    float autoDeathTime;

    [Server]
    void AutoMove()
    {
        if (Time.time > autoDeathTime)
        {
            TakeDamage(50000);
        }
        if (Time.time > autoMoveTimer)
        {
            autoTurret += UnityEngine.Random.Range(-90, 90);
            RotateUpperBody(autoTurret);

            float moveX = (UnityEngine.Random.Range(-10, 10) * 0.1f);
            float moveY = (UnityEngine.Random.Range(-10, 10) * 0.1f);
            Vector2 movement = new Vector2(moveX, moveY);
            movement.Normalize();

            if (ammo == 0)
            {
                Reload();
            }

            Walk(movement);
            autoMoveTimer = Time.time + UnityEngine.Random.Range(.5f, 4f);
        }
        FireWeapon();
    }

    void OnApplicationFocus(bool value)
    {
        m_focus = value;
    }

    Quaternion TrackRotate(Vector2 moveVect)
    {
        float degrees;
        if (moveVect.x < 0)
        {
            degrees = 360 - (Mathf.Atan2(moveVect.x, moveVect.y) * Mathf.Rad2Deg * -1);
        }
        else
        {
            degrees = Mathf.Atan2(moveVect.x, moveVect.y) * Mathf.Rad2Deg;
        }
        return Quaternion.AngleAxis(degrees, Vector3.back);
    }

    void UpdateClient()
    {
        if (isLocalPlayer)
        {
            UpdateCamera();
        }

        if (alive && Time.time > trackTimer && physBody.velocity.magnitude > 0.001f)
        {
            GameObject footprint = Instantiate(tracks, transform.position, TrackRotate(physBody.velocity));
            Destroy(footprint, 1.5f);
            trackTimer = Time.time + 0.25f;
        }

        if (!isLocalPlayer)
        {
            Vector3 trotationVector = new Vector3(0, 0, upperBodyAngle);
            upperBody.transform.rotation = Quaternion.Euler(trotationVector);
            return;
        }

        if (!m_focus)
            return;

        if (!alive)
        {
            return;
        }

        if (Input.GetKeyDown(KeyCode.LeftShift))
        {
            if (!sprinting)
            {
                sprinting = true;
            }
        }

        if (Input.GetKeyUp(KeyCode.LeftShift))
        {
            if (sprinting)
            {
                sprinting = false;
            }
        }

        HandlePlayerMovement();

        if (Input.GetMouseButtonDown(0))
        {
            if (!firingWeapon)
            {
                firingWeapon = true;
            }
        }

        if (Input.GetMouseButtonUp(0))
        {
            if (firingWeapon)
            {
                firingWeapon = false;
            }
        }

        if (Input.GetKeyDown(KeyCode.E))
        {
            if (!interacting)
            {
                interacting = true;
            }
        }

        if (Input.GetKeyUp(KeyCode.E))
        {
            if (interacting)
            {
                interacting = false;
            }
        }

        if (Input.GetKeyDown(KeyCode.R))
        {
            CmdReload();
        }

        Vector3 mouse_pos = Input.mousePosition;
        mouse_pos.z = 0.0f;
        Vector3 object_pos = Camera.main.WorldToScreenPoint(transform.position);
        mouse_pos.x = mouse_pos.x - object_pos.x;
        mouse_pos.y = mouse_pos.y - object_pos.y;
        float angle = Mathf.Atan2(mouse_pos.y, mouse_pos.x) * Mathf.Rad2Deg;
        Vector3 rotationVector = new Vector3(0, 0, angle);
        upperBody.transform.rotation = Quaternion.Euler(rotationVector);

        // point upper body at mouse
        if (Time.time > turrentSendTimer)
        {
            CmdRotateUpperBody(angle);
            turrentSendTimer = Time.time + turrentSendDelay;
        }

        if (firingWeapon)
        {
            FireWeapon();
        }

        if (interacting)
        {
            CmdInteract();
        }
    }

    void UpdateCamera()
    {
        Vector3 cpos = transform.position;
        cpos.z = Camera.main.transform.position.z;
        Camera.main.transform.position = cpos;
    }

    // Update is called once per frame
    void Update()
    {
        if (NetworkServer.active)
            UpdateServer();

        if (NetworkClient.active)
            UpdateClient();
    }
}