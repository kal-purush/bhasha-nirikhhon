using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Global_Variables : MonoBehaviour
{
    //PLAYER MOVEMENT SCRIPT ACCESSED VARIABLES 
    [Header("PLAYER MOVEMENT/HEALTH/FUEL/DAMAGE/CARRY CAPACITY")]
    [Header("Ship Settings")]
    public float driftFactor;
    public float accelerationFactor;
    public float turnFactor;
    public float maxSpeed;
    float accelerationInput;
    float steeringInput;
    float rotationAngle;
    float velocityVsUp;


    [Header("Player Health")]
    public int currentHealth;
    public int maxHealth;
    public int maxHealthUpgrade1;
    public int maxHealthUpgrade2;
    public int maxHealthUpgrade3;


    [Header("Player Fuel")]
    public int currentFuel;
    public int maxFuel;
    public int maxFuelUpgrade1;
    public int maxFuelUpgrade2;
    public int maxFuelUpgrade3;

    public int fuelDecreaseSeconds;
    public int fuelDecreaseAmountAfterSeconds;


    [Header("Player Order Carry Capacity")]
    public int baseCarryCapacity;
    public int carryCapacityUpgrade1;
    public int carryCarryCapacity2;
    public int carryCapacity3;

    [Header("Player Damage")]
    public int shipCollisionDamage;




    [Header("UPGRADE COSTS")]

    [Header("Health/Fuel/Carry Capacity Costs")]
    public int healthUpgrade1Cost;
    public int healthUpgrade2Cost;
    public int healthUpgrade3Cost;

    public int fuelUpgrade1Cost;
    public int fuelUpgrade2Cost;
    public int fuelUpgrade3Cost;

    public int carryCapictyUpgrade1Cost;
    public int carryCapictyUpgrade2Cost;
    public int carryCapictyUpgrade3Cost;

    [Header("Protection Upgrade Cost")]
    
    public int iceProectionUpgradeCost;
    public int heatProectionUpgradeCost;
    public int radiationProectionUpgradeCost;
    public int hardcoreProectionUpgradeCost;

    [Header("REFUEL AND REPAIR COSTS AND AMOUNT")]
    [Header("Refuel Cost")]
    public int baseRefuelCost;
    public int baseRefuelAmount;

    public int refuelCostPostFuelUpgrade1;
    public int refuelAmountPostFuelUpgrade1;

    public int refuelCostPostFuelUpgrade2;
    public int refuelAmountPostFuelUpgrade2;

    public int refuelCostPostFuelUpgrade3;
    public int refuelAmountPostFuelUpgrade3;

    [Header("Repair Cost")]
    public int baseRepairCost;
    public int baseRepairAmount;

    public int repairCostPostHealthUpgrade1;
    public int repairAmountPostHealthUpgrade1;

    public int repairCostPostHealthUpgrade2;
    public int repairAmountPostHealthUpgrade2;

    public int repairCostPostHealthUpgrade3;
    public int repairAmountPostHealthUpgrade3;


    [Header("ZONE DAMAGES")]
    [Header("Ice Zone")]
    public int shipMaxSpeedinIceZone;
   
    [Header("Heat Zone")]
    public int healthHeatDecreaseSeconds;
    public int healthHeatDecreaseAmountAfterSeconds;
    
    [Header("Hardcore Zone")]
    public int shipMaxSpeedinHardcoreZone;
    public int healthHardcoreDecreaseSeconds;
    public int healthHardcoreDecreaseAmountAfterSeconds;

    void Start()
    {
        
    }

    // Update is called once per frame
    void Update()
    {
        
    }
}