using UnityEngine;
using UnityEngine.UI;

public class AnimalPriceChanger : MonoBehaviour
{
    public GameObject animalList;

    public int ID;

    private Text price;

    private DebugShopAnimalList animal;

    void Start()
    {
       animal = animalList.GetComponent<DebugShopAnimalList>();
       price = gameObject.GetComponent<Text>();

       UpdatePriceText();
    }

    public void UpdatePriceText()
    {
        if (animal.animalList[ID].status.Rarity == 0)
        {
            price.text = "100";
        }
        else if (animal.animalList[ID].status.Rarity == 1)
        {
            price.text = "200";
        }
        else if (animal.animalList[ID].status.Rarity == 2)
        {
            price.text = "300";
        }
        else if (animal.animalList[ID].status.Rarity == 3)
        {
            price.text = "400";
        }
        else if (animal.animalList[ID].status.Rarity == 4)
        {
            price.text = "500";
        }
    }
}