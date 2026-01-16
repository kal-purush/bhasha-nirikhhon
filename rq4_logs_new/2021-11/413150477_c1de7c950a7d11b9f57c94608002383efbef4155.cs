using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.EventSystems;
using TMPro;

public class Button_Hover : MonoBehaviour ,IPointerEnterHandler, IPointerExitHandler
{
    public TextMeshProUGUI description;
    public TextMeshProUGUI heading;
    public UpgradeShop upgradeShop;
    public fuel_and_mechanic_Manager fuel_And_Mechanic_Manager;
    public bool mouse_over;

    public void start()
    {
        mouse_over = false;
        if (mouse_over == false)
        {
            heading.text = "";
            description.text = "";
        }
    }
    void Update()
    {
        if (mouse_over)
        {




            if (this.gameObject.name == "HealthUP1Button")
            {
                heading.text = "Health Upgrade 1";
                description.text = "Upgrade Health System to " + upgradeShop.maxHealthUpgrade1.ToString() + " max health for: $ " + upgradeShop.HealthUp1Cost.ToString();
            }

            if (this.gameObject.name == "HealthUP2Button")
            {
                heading.text = "Health Upgrade 2";
                description.text = "Upgrade Health System to " + upgradeShop.maxHealthUpgrade2.ToString() + " max health for: $ " + upgradeShop.HealthUp2Cost.ToString();

            }

            if (this.gameObject.name == "HealthUP3Button")
            {
                heading.text = "Health Upgrade 3";
                description.text = "Upgrade Health System to " + upgradeShop.maxHealthUpgrade3.ToString() + " max heath for: $ " + upgradeShop.HealthUp3Cost.ToString();
            }
            if (this.gameObject.name == "FuelUP1Button")
            {
                heading.text = "Fuel Upgrade 1";
                description.text = "Upgrade Fuel System to " + upgradeShop.maxFuelUpgrade1.ToString() + " max fuel for: $ " + upgradeShop.FuelUp1Cost.ToString();
            }

            if (this.gameObject.name == "FuelUP2Button")
            {
                heading.text = "Fuel Upgrade 2";
                description.text = "Upgrade Fuel System to " + upgradeShop.maxFuelUpgrade2.ToString() + " max fuel for: $ " + upgradeShop.FuelUp2Cost.ToString();
            }

            if (this.gameObject.name == "FuelUP3Button")
            {
                heading.text = "Fuel Upgrade 3";
                description.text = "Upgrade Fuel System to " + upgradeShop.maxFuelUpgrade3.ToString() + " max fuel for: $ " + upgradeShop.FuelUp3Cost.ToString();
            }

            if (this.gameObject.name == "Order Capacity Upgrade 1")
            {
                heading.text = "Order Upgrade 1";
                description.text = "Upgrade the max number of orders you can carry to " + upgradeShop.maxOrderCapacity.ToString() + " for: $ " + upgradeShop.carryCapUp1Cost.ToString();
            }

            if (this.gameObject.name == "Order Capacity Upgrade 2")
            {
                heading.text = "Order Upgrade 2";
                description.text = "Upgrade the max number of orders you can carry to " + upgradeShop.maxOrderCapacity.ToString() + " for: $ " + upgradeShop.carryCapUp2Cost.ToString();
            }

            if (this.gameObject.name == "Order Capacity Upgrade 3")
            {
                heading.text = "Order Upgrade 3";
                description.text = "Upgrade the max number of orders you can carry to " + upgradeShop.maxOrderCapacity.ToString() + " for: $ " + upgradeShop.carryCapUp3Cost.ToString();
            }

            if (this.gameObject.name == "IceProtection")
            {
                heading.text = "Ice Protection";
                description.text = "Protect your ship from slowing down from ice damage in the ice zone for: $ " + upgradeShop.icePro.ToString();
            }


            if (this.gameObject.name == "HeatProtection")
            {
                heading.text = "Heat Protection";
                description.text = "Protect your ship's health from burn damage in the heat zone for: $ " + upgradeShop.firePro.ToString();
            }


            if (this.gameObject.name == "RadiationProtection")
            {
                heading.text = "Radiation Protection";
                description.text = "Prevent your map system from being disabled from interference in the radiation zone for: $ " + upgradeShop.firePro.ToString();
            }


            if (this.gameObject.name == "RepairShipButton")
            {
                heading.text = "Repair Ship";
                description.text = "Restore your ship's health for: $ " + fuel_And_Mechanic_Manager.repairCost;

            }




            if (this.gameObject.name == "HardcoreButton")
            {
                heading.text = "Hardcore Protection";
                description.text = "Protect your ship from all the dangers in the hardcore zone for: $ " + upgradeShop.hardcorePro;
            }


            
        }

        else if (mouse_over == false)
        {
           
        }
    }



    public void OnPointerEnter(PointerEventData pointerEventData)
    {
        mouse_over = true;
        //Output to console the GameObject's name and the following message
       
    }

    public void OnPointerExit(PointerEventData eventData)
    {
        mouse_over = false;
        Debug.Log("The cursor exited the selectable UI element.");

    }

    public void exiting()
    {
        description.text = "";
        heading.text = "";
    }

}