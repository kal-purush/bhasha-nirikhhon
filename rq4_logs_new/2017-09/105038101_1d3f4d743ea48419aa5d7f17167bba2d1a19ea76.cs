using UnityEngine;

public class ShipController : MonoBehaviour 
{
    private ShipModel shipModel;

	void Awake () 
    {
        shipModel = new ShipModel();
	}

    public ShipModel GetShipModel()
    {
        return shipModel;
    }
	
}