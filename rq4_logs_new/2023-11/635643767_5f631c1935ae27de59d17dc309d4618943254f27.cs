using System.Collections;
using System.Collections.Generic;
using UnityEngine;
public class Chest : MonoBehaviour
{
    public enum ChestType
    {
        unlocked,
        silver,
        gold
    }
    public ChestType chestType;
    public GameObject[] regularItems;
    public GameObject[] silverItems;
    public GameObject[] goldItems;

    public Sprite[] sprites;
    private SpriteRenderer spriteRenderer;
    private bool canOpen = false;
    private bool opened = false;
    [SerializeField] private SpawnCurrency spawnCurrency = default;
    void Start()
    {
        spriteRenderer = GetComponent<SpriteRenderer>();
    }

    // Update is called once per frame
    void Update()
    {
        if (Input.GetKeyDown(KeyCode.F) && canOpen)
        {
            OpenChest();
        }
    }
    private void OpenChest()
    {
        int randomIndex;
        switch (chestType)
        {
            case ChestType.unlocked:
                spriteRenderer.sprite = sprites[3];
                randomIndex = Random.Range(0, regularItems.Length);
                Instantiate(regularItems[randomIndex], transform.position, Quaternion.identity);
                GetComponent<BoxCollider2D>().enabled = false;
                spawnCurrency.SpewOutCurrency();
                break;
            case ChestType.silver:
                if (PlayerCurrencies.Instance.hasSilverKey)
                {
                    spriteRenderer.sprite = sprites[1];
                    randomIndex = Random.Range(0, silverItems.Length);
                    Instantiate(silverItems[randomIndex], transform.position, Quaternion.identity);
                    GetComponent<BoxCollider2D>().enabled = false;
                    spawnCurrency.SpewOutCurrency();
                }
                break;
            case ChestType.gold:
                if (PlayerCurrencies.Instance.hasGoldKey)
                {
                    spriteRenderer.sprite = sprites[5];
                    randomIndex = Random.Range(0, goldItems.Length);
                    Instantiate(goldItems[randomIndex], transform.position, Quaternion.identity);
                    GetComponent<BoxCollider2D>().enabled = false;
                    spawnCurrency.SpewOutCurrency();
                }
                break;
        }
    }
    private void OnCollisionEnter2D(Collision2D collision)
    {
        if (collision.gameObject.CompareTag("Player"))
        {
            Player.Instance.SetInteractPromtTextActive(true);
            canOpen = true;
        }
    }
    private void OnCollisionExit2D(Collision2D collision)
    {
        if (collision.gameObject.CompareTag("Player"))
        {
            Player.Instance.SetInteractPromtTextActive(false);
            canOpen = false;

        }
    }
}