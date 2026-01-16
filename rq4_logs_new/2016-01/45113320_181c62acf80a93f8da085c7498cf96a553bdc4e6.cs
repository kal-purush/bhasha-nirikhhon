using UnityEngine;
using System.Collections;

public class MiniGun : AbstractWeapon {
    public override void Init()
    {
        type = EnumerationGun.GunType.MINIGUN;
        maxAmmo = 100.0f;
        chargeurMax = 100.0f;
        chargeurCurrent = 0.0f;
        damage = 1.0f;
        fireRate = 0.1f;
        reloadingTime = 5.0f;
        isAuto = true;
        cursor = Resources.Load<Sprite>("Graph/cursor/Cursor8");
        base.Init();
    }

    public override void GiveTo(Player player)
    {
        Destroy(player.currentWeapon);
        AbstractWeapon nextGun = player.gameObject.AddComponent<MiniGun>();
        AttachTo(player.gameObject, typeof(MiniGun));
        if(gameObject.tag != "Player")
            Destroy(gameObject);
    }
}