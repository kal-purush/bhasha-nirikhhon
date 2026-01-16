using System.Collections;
using System.Collections.Generic;
using UnityEngine;


public class PlayerCheckpoint : MonoBehaviour
{
    public static int balance = 0;
    public static float health = 100;
    public static HashSet<Item.ItemType> items;
    public static Vector3 location;
    public static Quaternion rotation;

    // Start is called before the first frame update
    void Start()
    {
        
    }

    // Update is called once per frame
    void Update()
    {
        
    }

    private void OnTriggerEnter(Collider other)
    {
        if (other.CompareTag("Player"))
        {
            RecordCheckpoint();
        }
    }

    public static void RecordCheckpoint()
    {
        Debug.Log("record checkpoint");
        balance = PlayerBankAccount.GetCurrentBalance();
        health = PlayerHealth.GetCurrentHealth();
        items = Item.GetItemsPurchased();
        location = GameObject.FindGameObjectWithTag("Player").transform.position;
        rotation = GameObject.FindGameObjectWithTag("Player").transform.rotation;
    }

    public static void RevertToCheckpoint()
    {
        Debug.Log("revert called");
        PlayerHealth.SetCurrentHealth(health);

        PlayerBankAccount.SetCurrentBalance(balance);
        PlayerBankAccount.UpdateBalanceText();
            Item.SetItemsPurchased(items);

        GameObject player = GameObject.FindGameObjectWithTag("Player");
        CharacterController controller = player.GetComponent<CharacterController>();
        player.transform.position = location;
        player.transform.rotation = rotation;
        controller.enabled = true;
    }
}