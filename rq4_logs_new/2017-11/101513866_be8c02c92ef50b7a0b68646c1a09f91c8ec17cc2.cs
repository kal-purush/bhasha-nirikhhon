using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Cursor_Image : MonoBehaviour
{
    public Sprite wateringCanCursor;
    public Sprite fireSeedPacketCursor;
    public Sprite iceSeedPacketCursor;
    public Sprite voidSeedPacketCursor;
    public Sprite defaultCursor;
    public GameObject select;
    public Vector3 updatePosition;
    float moveSpeed = 10;

	// Use this for initialization
	void Start ()
    {
        Cursor.visible = false;
	}
	
	// Update is called once per frame
	void Update ()
    {
        Cursor.visible = false;
        updatePosition = Input.mousePosition;
        updatePosition = Camera.main.ScreenToWorldPoint(updatePosition);
        updatePosition = new Vector3(updatePosition.x, updatePosition.y, -3);
        transform.position = Vector2.Lerp(transform.position, updatePosition, moveSpeed);
        transform.position = new Vector3(transform.position.x, transform.position.y, 0f);

        if(Input.GetMouseButtonDown(0) && GetComponent<SpriteRenderer>().sprite.name != wateringCanCursor.name)
        {
            transform.eulerAngles = new Vector3(transform.eulerAngles.x, transform.eulerAngles.y, transform.eulerAngles.z + 20f);
        }
        else if (Input.GetMouseButtonDown(1) && GetComponent<SpriteRenderer>().sprite.name == wateringCanCursor.name)
        {
            transform.eulerAngles = new Vector3(transform.eulerAngles.x, transform.eulerAngles.y, transform.eulerAngles.z - 20f);
        }

        if (Input.GetMouseButtonUp(0) && GetComponent<SpriteRenderer>().sprite.name != wateringCanCursor.name)
        {
            transform.eulerAngles = new Vector3(transform.eulerAngles.x, transform.eulerAngles.y, transform.eulerAngles.z - 20f);
        }
        else if (Input.GetMouseButtonUp(1) && GetComponent<SpriteRenderer>().sprite.name == wateringCanCursor.name)
        {
            transform.eulerAngles = new Vector3(transform.eulerAngles.x, transform.eulerAngles.y, transform.eulerAngles.z + 20f);
        }
    }

    private void OnCollisionStay2D(Collision2D coll)
    {
        if (coll.gameObject.tag == "FarmTile")
        {
            if (coll.gameObject.GetComponent<Farm_Controller>().isPlanted)
            {
                GetComponent<SpriteRenderer>().sprite = wateringCanCursor;
            }
            else
            {
                switch (Game_Manager.Instance.currentPlantSelection)
                {
                    case Game_Manager.PlantType.FIRE:
                        GetComponent<SpriteRenderer>().sprite = fireSeedPacketCursor;
                        break;
                    case Game_Manager.PlantType.ICE:
                        GetComponent<SpriteRenderer>().sprite = iceSeedPacketCursor;
                        break;
                    case Game_Manager.PlantType.VOID:
                        GetComponent<SpriteRenderer>().sprite = voidSeedPacketCursor;
                        break;
                }
            }
        }
    }

    private void OnCollisionEnter2D(Collision2D coll)
    {
        if (coll.gameObject.tag == "FarmTile")
        {
            if(coll.gameObject.GetComponent<Farm_Controller>().isPlanted)
            {
                GetComponent<SpriteRenderer>().sprite = wateringCanCursor;
            }
            else
            {
                switch(Game_Manager.Instance.currentPlantSelection)
                {
                    case Game_Manager.PlantType.FIRE:
                        GetComponent<SpriteRenderer>().sprite = fireSeedPacketCursor;
                        break;
                    case Game_Manager.PlantType.ICE:
                        GetComponent<SpriteRenderer>().sprite = iceSeedPacketCursor;
                        break;
                    case Game_Manager.PlantType.VOID:
                        GetComponent<SpriteRenderer>().sprite = voidSeedPacketCursor;
                        break;
                }
            }
        }
    }

    private void OnCollisionExit2D(Collision2D coll)
    {
        GetComponent<SpriteRenderer>().sprite = defaultCursor;
    }
}