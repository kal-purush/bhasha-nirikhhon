using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Baloon : MonoBehaviour
{
    private Rigidbody balloonRigidBody;
    private float BaloonWeight;
    private float CartWeight;
    
    [SerializeField]private float BallonTemperatureKelvin = GROUND_TEMP_KELVIN;
    [SerializeField] private float DebugCTemp;
    private const float BALLOON_COOL_TEMP_KELVIN = GROUND_TEMP_KELVIN;
    private const float GROUND_TEMP_KELVIN = 273f + 20f; // 20C

    private const float AIR_PRESSURE_ATM = 1.03f; // in atm
    private const float BALOON_VOLUME_LITRES = 2800000f; // 2.8 million litres

    private const float ATMO_CONSTANT = 0.08206f;


    // Start is called before the first frame update
    void Start()
    {
        var baloon = GameObject.FindGameObjectWithTag("Baloon");
        var cart = GameObject.FindGameObjectWithTag("Cart");
        balloonRigidBody = baloon.GetComponentInChildren<Rigidbody>();

        BaloonWeight = baloon.GetComponentInChildren<Rigidbody>().mass;
        CartWeight = cart.GetComponentInChildren<Rigidbody>().mass;
    }

    // Update is called once per frame
    void Update()
    {
        var test1 = GetMassOfAir(GetDensityOfCoolAirInBalloon());
        var test2 = GetMassOfAir(GetDensityOfAirInBalloon());
        //Debug.Log( (GetMassOfAir(GetDensityOfCoolAirInBalloon()) - GetMassOfAir(GetDensityOfAirInBalloon())) / 1000f);
    }

    void FixedUpdate()
    {
        var kgOfLift = (GetMassOfAir(GetDensityOfCoolAirInBalloon()) - GetMassOfAir(GetDensityOfAirInBalloon())) /
                       100000f;
        var newtonsOfLift = kgOfLift * -Physics.gravity.y;
        var msminus2 = Mathf.Clamp(newtonsOfLift / BaloonWeight, 0f, float.PositiveInfinity);

        DebugCTemp = BallonTemperatureKelvin - 273f;
        Debug.Log(msminus2);
        balloonRigidBody.AddForce(0f, msminus2, 0f);
    }

    /// <summary>
    /// Get how many moles the cool air is.
    /// </summary>
    /// <returns>Moles of air of 80N/20O in moles.</returns>
    float GetMolesOfCoolAirInBaloon()
    {
        return (AIR_PRESSURE_ATM * BALOON_VOLUME_LITRES) / (ATMO_CONSTANT * BALLOON_COOL_TEMP_KELVIN);
    }

    /// <summary>
    /// Get desinity of air in a cool hot air balloon.
    /// </summary>
    /// <returns>Density of air at cool temp in grams over liters.</returns>
    private float GetDensityOfCoolAirInBalloon()
    {
        return (AIR_PRESSURE_ATM * GetMolesOfCoolAirInBaloon()) / (ATMO_CONSTANT * BALLOON_COOL_TEMP_KELVIN);
    }

    /// <summary>
    /// How many moles is the air in the balloon currently.
    /// </summary>
    /// <returns>Moles of air in balloon.</returns>
    private float GetCurrentMolesInBalloon()
    {
        return (AIR_PRESSURE_ATM * BALOON_VOLUME_LITRES) / (ATMO_CONSTANT * BallonTemperatureKelvin);
    }

    /// <summary>
    /// Density of air in balloon.
    /// </summary>
    /// <returns>Density of air in grams over liters.</returns>
    private float GetDensityOfAirInBalloon()
    {
        return (AIR_PRESSURE_ATM * GetCurrentMolesInBalloon()) / (ATMO_CONSTANT * BallonTemperatureKelvin);
    }

    /// <summary>
    /// Get mass of air in balloon at density. Volume is factored in during calculation.
    /// </summary>
    /// <param name="density"></param>
    /// <returns>Grams of weight in balloon.</returns>
    private float GetMassOfAir(float density)
    {
        return density * BALOON_VOLUME_LITRES;
    }
}