using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using TMPro;
using Cinemachine;
using System.Threading.Tasks;

public class CollectablesCounter : MonoBehaviour{
    private int requiredFuel = 8;
    private int requiredMessage = 7;

    public int currentFuel = -1;
    public int currentMessage = -1;

    public CinemachineVirtualCamera ShipDoorVCam;
    public SlideDoor Door;

    [SerializeField] TextMeshProUGUI FuelCountText;
    [SerializeField] TextMeshProUGUI MessageCountText;

    void Start() {
        UpdateFuel();
        //UpdateMessage();
    }

    public void UpdateFuel(){
        currentFuel++;
        FuelCountText.text = $"{currentFuel}/{requiredFuel}";
    }

    public async void UpdateMessage(){
        currentMessage++;
        MessageCountText.text = $"{currentMessage}/{requiredMessage}";
        if (currentMessage == requiredMessage){
            Time.timeScale = 0f;
            await Task.Delay(1000);
            ShipDoorVCam.Priority = 20;
            await Task.Delay(800);
            Door.Open();
            await Task.Delay(3000);
            ShipDoorVCam.Priority = 10;
            Time.timeScale = 1f;
        }
    }
}