using Rhyth.BTree;
using System.Collections.Generic;
using UnityEngine;

public class ChangeWeaponNode : BNodeAdapter
{
    public override int MaxNumberOfChildren => 0;
    public override string StringToolTip => "Replaces the current equipped weapon with another in the given time.";

    [SerializeField] private bool selectRandomWeapon;
    [SerializeField] private int weaponIndex;
    [SerializeField] private float timeBeforeChange;
    [SerializeField] private float timeAfterChange;

    private Weapon selectedWeapon;
    private float timer;

    private Enemy onEnemy;

    private enum CurrentState
    {
        Setup, BeforeChange, AfterChange
    }
    private CurrentState state;

    public override void InnerSetup()
    {
        onEnemy = Brain.GetComponent<Enemy>();
    }

    public override void InnerBeginn()
    {
        state = CurrentState.Setup;
        timer = timeBeforeChange;
    }

    public override void Update()
    {
        switch (state)
        {
            case CurrentState.Setup:
                if (selectRandomWeapon)
                {
                    selectedWeapon = onEnemy.carryingWeapons[Random.Range(0, onEnemy.carryingWeapons.Length)];
                }
                else
                {
                    if (weaponIndex < 0 || weaponIndex >= onEnemy.carryingWeapons.Length)
                    {
                        CurrentStatus = Status.Failure;
                        return;
                    }
                    selectedWeapon = onEnemy.carryingWeapons[weaponIndex];
                }
                break;

            case CurrentState.BeforeChange:
                timer -= Time.deltaTime;
                if (timer > 0)
                    return;

                onEnemy.EquippedWeapon.Swap(selectedWeapon, false);
                timer = timeAfterChange;
                break;

            case CurrentState.AfterChange:
                timer -= Time.deltaTime;
                if (timer > 0)
                    return;

                CurrentStatus = Status.Success;
                break;

        }
    }

    protected override BNode InnerClone(Dictionary<Value, Value> originalValueForClonedValue)
    {
        ChangeWeaponNode cwn = CreateInstance<ChangeWeaponNode>();
        cwn.selectRandomWeapon = selectRandomWeapon;
        cwn.weaponIndex = weaponIndex;
        cwn.timeBeforeChange = timeBeforeChange;
        cwn.timeAfterChange = timeAfterChange;
        return cwn;
    }
}