using System.Collections;
using System.Collections.Generic;
using UnityEngine;

#if UNITY_EDITOR
using UnityEditor;
#endif

public class OnRailsOrbit : MonoBehaviour
{
    struct Constants    //Put somewhere else
    {
        public const float G = 6.67f;
    }

    struct Math         //Put somewhere else
    {
        public const float TAU = 6.28318530718f;
    }

    //Orbital Keplerian Parameters
    [SerializeField] float semiMajorAxis = 20f;        //a - size
    [SerializeField] [Range(0f, 0.99f)] float eccentricity;             //e - shape
    [SerializeField] [Range(0f, Math.TAU)] float inclination = 0f;         //i - tilt
    [SerializeField] [Range(0f, Math.TAU)] float longitudeOfAcendingNode;  //n - swivel
    [SerializeField] [Range(0f, Math.TAU)] float argumentOfPeriapsis;      //w - position
    [SerializeField] float meanLongitude;             //L - offset
    [SerializeField] OnRailsReferenceBody referenceBody;
    [SerializeField] float meanAnomaly;
    [Space]
    //Settings
    [SerializeField] float accuracyTolerance = 1e-6f;
    [SerializeField] int maxIterations = 5;           //usually converges after 3-5 iterations.

    //Numbers which only change if orbit or mass changes
    [HideInInspector] [SerializeField] float mu;
    [HideInInspector] [SerializeField] float n, cosLOAN, sinLOAN, sinI, cosI, trueAnomalyConstant;

    /*private void OnValidate() => orbitalPoints.Clear();*/
    void Awake() => CalculateSemiConstants();
    public float F(float E, float e, float M)  //Function f(x) = 0
    {
        return (M - E + e * Mathf.Sin(E));
    }
    public float DF(float E, float e)      //Derivative of the function
    {
        return (-1f) + e * Mathf.Cos(E);
    }
    public void CalculateSemiConstants()    //Numbers that only need to be calculated once if the orbit doesn't change.
    {
        mu = Constants.G * referenceBody.mass;
        n = Mathf.Sqrt(mu / Mathf.Pow(semiMajorAxis, 3));
        trueAnomalyConstant = Mathf.Sqrt((1 + eccentricity) / (1 - eccentricity));
        cosLOAN = Mathf.Cos(longitudeOfAcendingNode);
        sinLOAN = Mathf.Sin(longitudeOfAcendingNode);
        cosI = Mathf.Cos(inclination);
        sinI = Mathf.Sin(inclination);
    }

    void Update()
    {
        CalculateSemiConstants();

        meanAnomaly = (float)(n * (Time.time - meanLongitude));

        float E1 = meanAnomaly;   //initial guess
        float difference = 1f;
        for (int i = 0; difference > accuracyTolerance && i < maxIterations; i++)
        {
            float E0 = E1;
            E1 = E0 - F(E0, eccentricity, meanAnomaly) / DF(E0, eccentricity);
            difference = Mathf.Abs(E1 - E0);
        }
        float EccentricAnomaly = E1;

        float trueAnomaly = 2 * Mathf.Atan(trueAnomalyConstant * Mathf.Tan(EccentricAnomaly / 2));
        float distance = semiMajorAxis * (1 - eccentricity * Mathf.Cos(EccentricAnomaly));

        float cosAOPPlusTA = Mathf.Cos(argumentOfPeriapsis + trueAnomaly);
        float sinAOPPlusTA = Mathf.Sin(argumentOfPeriapsis + trueAnomaly);

        float x = distance * ((cosLOAN * cosAOPPlusTA) - (sinLOAN * sinAOPPlusTA * cosI));
        float z = distance * ((sinLOAN * cosAOPPlusTA) + (cosLOAN * sinAOPPlusTA * cosI));      //Switching z and y to be aligned with xz not xy
        float y = distance * (sinI * sinAOPPlusTA);

        transform.position = new Vector3(x, y, z) + referenceBody.transform.position;
    }
}