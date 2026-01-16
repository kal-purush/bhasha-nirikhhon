using UnityEngine;
using System.Collections;

/*  ToDo:
*   DecreaseStamina   
*/

public class AddStamina : MonoBehaviour
{
    public float stamina = 10;

    private PlayerController playerController;
	// Use this for initialization
	void Start ()
    {
        playerController = GameObject.FindGameObjectWithTag(MyConst.player).GetComponent<PlayerController>();
    }

    void OnCollisionEnter2D(Collision2D col)
    {
        if (col.gameObject.tag == MyConst.player)
        {
            playerController.IncreaseStamina(stamina);
            Destroy(gameObject);
        }
    }
}