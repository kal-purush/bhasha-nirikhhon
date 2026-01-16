using UnityEngine;
using System.Collections;

public class falconSpriteRotation : MonoBehaviour {
	
	// Update is called once per frame
	void Update () {
        float xVelo = GetComponent<UserInputFalconRPhysics>().movementVect.x;
        float yVelo = GetComponent<UserInputFalconRPhysics>().movementVect.y;
        float hypotVeloHalf = (Mathf.Sqrt(Mathf.Pow(xVelo, 2) + Mathf.Pow(yVelo, 2))) / 2;
        float radAngle = Mathf.Acos((Mathf.Pow(yVelo, 2)) / (2 * hypotVeloHalf * yVelo));
        float degAngle = radAngle * Mathf.Rad2Deg;
        transform.eulerAngles = new Vector3(0, 0, 90-degAngle);
	}
}