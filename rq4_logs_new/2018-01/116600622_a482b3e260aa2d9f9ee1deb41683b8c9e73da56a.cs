using UnityEngine;
using System.Collections;
using UnityEngine.Networking;

public class FighterCombat : NetworkBehaviour
{
    public GameObject turret;
    public Transform muzzle;
    public GameObject bulletPrefab;
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
    
    /*[SyncVar]
    public int ammunitionTurret = 100;

    [SyncVar]
    public int ammunitionMG = 1000;*/

    [SyncVar]
    public bool alive = true;

    [SyncVar]
    public bool equipped = true;

    /*[SyncVar]
    public bool firingMG = false;

    [SyncVar]
    public float armorFront = 1.0f;

    [SyncVar]
    public float armorBack = 1.0f;

    [SyncVar]
    public float armorLeft = 1.0f;

    [SyncVar]
    public float armorRight = 1.0f;*/

    [SyncVar]
    public string fighterName;

    [SyncVar]
    public string fighterType;

    [SyncVar]
    public int team;

    float fireTurretTimer;
    float regenTimer;
    float machineGunTimer;
    float deathTimer;

    [Server]
    void Respawn()
    {
        InitializeFromFighterType(ft);
        transform.position = Vector3.zero;
        transform.rotation = Quaternion.identity;
        alive = true;
        EventRespawn();
    }

    [Server]
    public void InitializeFromFighterType(FighterType newFT)
    {
        ft = newFT;
        fighterType = ft.fighterTypeName;
        health = ft.maxHealth;
        energy = 0;
        team = (int)(Time.time) % 2;

        GetComponent<SpriteRenderer>().sprite = ft.skinBody;
        /*turret.GetComponent<SpriteRenderer>().sprite = ft.skinTurret;*/
    }

    public override void OnStartClient()
    {
        if (NetworkServer.active)
            return;

        //TODO
        FighterType found = FighterTypeManager.Lookup(fighterType);
        ft = found;

        GetComponent<SpriteRenderer>().sprite = ft.skinBody;
        /*turret.GetComponent<SpriteRenderer>().sprite = ft.skinTurret;*/
    }

    [ServerCallback]
    void Update()
    {
        if (!alive)
        {
            if (Time.time > deathTimer)
            {
                Respawn();
            }
            return;
        }

        // energy recharges over time
        if (Time.time > regenTimer)
        {
            if (energy < 100)
            {
                energy += ft.energyRegen;
            }
            regenTimer = Time.time + 1.0f;
        }


        /*if (Time.time > machineGunTimer && firingMG)
        {
            // cast ray to hit stuff
            ammunitionMG -= 1;
            machineGunTimer += 0.33f;
        }*/
    }

    public bool CanFireTurret()
    {
        /*if (!equipped)
            return false;

        if (!alive)
            return false;*/

        return true;
    }


    float HitArmour(int side, float amount)
    {
        /*switch (side)
        {
            case 0:
                armorFront -= amount;
                return armorFront;

            case 1:
                armorRight -= amount;
                return armorRight;

            case 2:
                armorBack -= amount;
                return armorBack;

            case 3:
                armorLeft -= amount;
                return armorLeft;
        }*/
        return 0.0f;
    }

    [Server]
    public void GotHitByMissile(int side, int damage, int team)
    {
        if (this.team == team)
        {
            return;
        }

        EventTakeDamage(side, damage);
        float armor = HitArmour(side, damage / 100.0f);
        if (armor <= 0.0f)
            TakeDamage(damage);
        else
            TakeDamage(damage / 10);
    }

    [Server]
    public void GotHitByMachineGun()
    {
        TakeDamage(2);
    }

    [Server]
    void TakeDamage(int amount)
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
            GetComponent<Rigidbody2D>().velocity = Vector2.zero;
            // play explosion!
            // set death timer for respawn
            EventDie();
            deathTimer = Time.time + 5.0f;
        }
    }

    [Command]
    public void CmdFireTurret()
    {
        if (PlayGame.GetComplete())
            return;
        
        if (!CanFireTurret())
            return;

        /*heat += ft.fireHeat;
        ammunitionTurret -= 1;
        fireTurretTimer = Time.time + ft.fireRateTurret;*/

        GameObject bullet = (GameObject)GameObject.Instantiate(bulletPrefab, muzzle.position, muzzle.rotation);
        bullet.GetComponent<Rigidbody2D>().velocity = muzzle.right * 30;
        bullet.GetComponent<Missile>().damage = 1;
        bullet.GetComponent<Missile>().team = team;
        NetworkServer.Spawn(bullet);
    }

    [Command]
    public void CmdBeginFireMachineGun()
    {
        if (PlayGame.GetComplete())
            return;

        if (!alive)
            return;

        /*if (ammunitionMG > 0)
        {
            firingMG = true;
        }*/
    }

    [Command]
    public void CmdStopFireMachineGun()
    {
        /*firingMG = false;*/
    }

    [Command]
    public void CmdSetName(string name)
    {
        fighterName = name;
    }

    [Command]
    public void CmdKillSelf()
    {
        TakeDamage(1000000);
    }

    public override void OnStartLocalPlayer()
    {
        //CmdSetName(Manager.singleton.fighterName + "-" + ft.fighterTypeName);
    }
}