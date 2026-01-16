using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.InputSystem;
using UnityEngine.InputSystem.UI;

public class Player : MonoBehaviour
{
    private string number;

    public GameObject obj;
    public PlayerWorld world;
    public PlayerData data;
    public InputHandling inputHandling;
    public PlayerInput input;

    public GameObject EventSystemObj;
    public MultiplayerEventSystem eventSystem;
    public InputSystemUIInputModule inputSystem;

    public GameObject fleetMenuObj;
    public FleetMenuScript fleetMenu;

    public GameObject cameraVehicleObj;
    public VehicleBehavior vehicle;
    public CameraBehavior cameraBehavior;

    private void Awake()
    {
        number = this.name[this.name.Length -1].ToString();
        obj = GameObject.Find(this.name);
        world = obj.GetComponent<PlayerWorld>();
        data = world.playerData;
        inputHandling = obj.GetComponent<InputHandling>();
        input = obj.GetComponent<PlayerInput>();

        EventSystemObj = GameObject.Find("EventSystem" + number);
        eventSystem = EventSystemObj.GetComponent<MultiplayerEventSystem>();
        inputSystem = EventSystemObj.GetComponent<InputSystemUIInputModule>();

        fleetMenuObj = GameObject.Find("FleetMenu" + number);
        fleetMenu = fleetMenuObj.GetComponent<FleetMenuScript>();

        cameraVehicleObj = GameObject.Find("CameraVehicle" + number);
        vehicle = cameraVehicleObj.GetComponent<VehicleBehavior>();
        cameraBehavior = cameraVehicleObj.GetComponent<CameraBehavior>();
    }

    private void Start()
    {
        
    }
}