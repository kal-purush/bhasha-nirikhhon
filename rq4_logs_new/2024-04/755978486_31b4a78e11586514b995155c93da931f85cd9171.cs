using UnityEngine;
using Assets.Scripts.Combat;

public class BossPuppeteerHackCompletion : HackCompletion
{
    public override void OnHackSuccess()
    {
        return;
    }
    public override void OnHackFail()
    {
        return;
    }
    public override void OnComboHackSuccess(int comboCount)
    {
        // set gun ammo to comboCount
        if (TryGetComponent(out Gun gun))
        {
            gun.CurrentAmmo = (uint)comboCount;
        }
        else
        {
            Debug.LogError("BossPuppeteerHackCompletion: OnComboHackSuccess: Gun component not found");
        }
        return;
    }
    public override void OnComboHackFail(int comboCount)
    {
        return;
    }
}