using Unity.VisualScripting;
using UnityEngine;

public class PlayerEnergy : MonoBehaviour
{
    public float maxEnergy = 10;
    public float energyToActivate = 1;
    public float energyPerSecond = 3;
    public float energyRegenerationPerSecond = 1;
    
    public float speedUp = 6;

    private MoveController _moveController;

    private bool _activated;

    private bool Activated
    {
        get => _activated;
        set
        {
            if (_activated != value)
            {
                _moveController.speed += (value ? 1 : -1) * speedUp;
            }
            _activated = value;
        }
    }

    private float _energy;
    
    void Start()
    {
        _moveController = GetComponent<MoveController>();

        _energy = maxEnergy;
    }

    private void Update()
    {
        if (Input.GetKeyDown(KeyCode.Z))
        {
            if (!Activated)
            {
                if (_energy < energyToActivate)
                {
                    return;
                }

                _energy -= energyToActivate;
            }

            Activated = !Activated;
        }

        if (Activated)
        {
            if (_energy >= Time.deltaTime * energyPerSecond)
            {
                _energy -= Time.deltaTime * energyPerSecond;
            }
            else
            {
                Activated = false;
            }
        }
        else
        {
            _energy = Mathf.Min(
                _energy + Time.deltaTime * energyRegenerationPerSecond, maxEnergy);
        }
    }
}