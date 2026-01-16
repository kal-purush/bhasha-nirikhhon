using UnityEngine;
using System.Collections;

public class CaravanPlayer : MonoBehaviour
{
	public Transform trans = null;
    Card handPlacement = null;

	void Start ()
    {
	
	}
	
	void Update ()
    {
		if (Input.GetMouseButtonDown (1))
		{
			RaycastHit hit;
			Ray ray = Camera.main.ScreenPointToRay(Input.mousePosition);
			
			if (Physics.Raycast(ray, out hit, 100f))
			{
				// Here we'll check if we have a card already.
				// If we do have a card we'll check where we're putting it.

				Card valueCheck = (Card)hit.collider.gameObject.GetComponent<Card>();

				if (valueCheck != null)
				{
					handPlacement = valueCheck;
					// 6 = players hand, 0 = ID within the stack, 4 = dest location, 0 = index within location.
					trans.gameObject.GetComponent<CaravanBoard>().makeMove(6, 0, 4, 0);
				}
			}
		}
	}
}