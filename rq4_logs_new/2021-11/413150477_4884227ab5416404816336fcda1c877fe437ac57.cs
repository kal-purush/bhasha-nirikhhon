using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class upgrade_shop_opener : MonoBehaviour
{
    // Start is called before the first frame update
    public GameObject repairPanel;
    public bool isAtShop;
    public bool shopOP;
    public Text instruction;

    void Start()
    {
        repairPanel.SetActive(false);
    }

    // Update is called once per frame

    void Update()
    {
        /// when the rocket is in the vacinity of the mechanic, this is to open and close the panel with the options
        if (isAtShop)
        {
            if (Input.GetKeyDown(KeyCode.E) && !shopOP)
            {
                repairPanel.SetActive(true);
                shopOP = true;
                instruction.text = "Press E to close shop".ToString();
                Debug.Log("OPEN");
            }
            else if (Input.GetKeyDown(KeyCode.E) && shopOP)
            {
                repairPanel.SetActive(false);
                shopOP = false;
                instruction.text = "Press E to open shop".ToString();
                Debug.Log("CLOSE");
            }
        }
    }

    private void OnTriggerEnter2D(Collider2D collision)
    {
        if (collision.gameObject.tag == "Player")
        {
            instruction.text = "Press E to open shop".ToString();
            isAtShop = true;
        }
    }

    private void OnTriggerExit2D(Collider2D collision)
    {
        if (collision.gameObject.tag == "Player")
        {
            repairPanel.SetActive(false);
            instruction.text = null;
        }
    }
}
