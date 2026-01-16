using System.Collections;
using System.Collections.Generic;
using UnityEngine;
[RequireComponent(typeof(Rigidbody2D))]
public class OrbitHandler : MonoBehaviour {

    public static float GravityConstant = 6.6f*Mathf.Pow(10,-11);

    public Rigidbody2D PrimaryBody;
    Rigidbody2D SecondaryBody;//its own rigidbody

    float GM ;

    public float Eccentricity;
    public Vector2 EccentricityVector;
    public Vector2 EccentricityVectorNorm;
    public float EccentricityAngle;

    public float Apoapsis;
    public float Periapsis;

    public float SemiMajorAxis;
    public float SemiMinorAxis;

    public float TrueAnomoly;
    public float EccentricAnomoly;
    public float MeanAnomoly;
    public float MeanAnomolyAtEpoch;
    public float MeanMotion;

    public float Radius;

	void Start ()
    {

        GM = PrimaryBody.mass*GravityConstant;
        SecondaryBody = GetComponent<Rigidbody2D>();
        SecondaryBody.isKinematic = true;

        float Velocity = SecondaryBody.velocity.magnitude;
        float VelocitySq = SecondaryBody.velocity.sqrMagnitude;
        Vector2 RelativePos = SecondaryBody.position - PrimaryBody.position;
        Radius = RelativePos.magnitude;

        EccentricityVector = RelativePos * (VelocitySq/GM-1/Radius);
        EccentricityVectorNorm = EccentricityVector.normalized;
        Eccentricity = EccentricityVector.magnitude;
        EccentricityAngle = Mathf.Atan2(EccentricityVector.y,EccentricityVector.x);

        float Energy = VelocitySq/2-GM/Radius;
        SemiMajorAxis = GM/(2*Energy);
        SemiMinorAxis = SemiMajorAxis * Mathf.Sqrt(1-Sq(Eccentricity));

        Apoapsis = (1 + Eccentricity) * SemiMajorAxis;
        Periapsis = (1 - Eccentricity) * SemiMajorAxis;
	}
	
	void Update ()
    {
		
	}

    void EccentricFromTrue()
    {
        
    }
    void MeanFromEccentric()
    {
        MeanAnomoly = EccentricAnomoly+Mathf.Sin(EccentricAnomoly);
    }
    void EccentricFromMean()
    {

    }
    void TrueFromEccentric()
    {
        TrueAnomoly = Mathf.Atan2(Mathf.Sqrt(1 - Sq(Eccentricity)) * Mathf.Sin(EccentricAnomoly), Eccentricity + Mathf.Cos(EccentricAnomoly));
    }

    float getRadiusFromAngle(float Theta)//relative to apoapsis
    {
        return 1;
    }
    public float getRadiusFromAngleRotated(float Theta)//relative to x-axis
    {
        return 1;
    }
    Vector2 RotateToWorld(Vector2 V)
    {
        return new Vector2(
            V.x * EccentricityVectorNorm.x - V.y * EccentricityVectorNorm.y,
            V.y * EccentricityVectorNorm.x + V.x * EccentricityVectorNorm.y
            );
    }
    float Sq(float x)
    {
        return x * x;
    }
}