using System.Collections;
using System.Collections.Generic;
using UnityEngine;

using UnityEngine.UI;

public class PlayerStats : MonoBehaviour {

    public float Health = 100;
    public float Energy = 100;
    public float Mass = 100;

    public Image HealthBar;
    public Image EnergyBar;
    public Image MassBar;

    public bool ModifyHealth(float amount)
    {
        if(Health + amount <= 100 && Health + amount >= 0)
        {
            Health += amount;
            HealthBar.fillAmount = Health / 100;
            return true;
        }
        else
        {
            return false;
        }
    }

    public bool ModifyEnergy(float amount)
    {
        if (Energy + amount <= 100 && Energy + amount >= 0)
        {
            Energy += amount;
            EnergyBar.fillAmount = Energy / 100;
            return true;
        }
        else
        {
            return false;
        }
    }

    public bool ModifyMass(float amount)
    {
        if (Mass + amount <= 100 && Mass + amount >= 0)
        {
            Mass += amount;
            MassBar.fillAmount = Mass / 100;
            return true;
        }
        else
        {
            return false;
        }
    }


}