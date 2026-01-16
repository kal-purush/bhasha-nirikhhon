using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

namespace Assets.Scripts
{
    public class GameManager : MonoBehaviour
    {
        public CarMovement playerCar;
        public UIManager ui;
        public int winLaps;

        void Start()
        {
            ui.SetMaxLaps(winLaps);
        }

        // Update is called once per frame
        void Update()
        {
            ui.SetLaps(playerCar.GetDoneCheckpointCount() / winLaps);
            ui.SetSpeed(playerCar.GetSpeed());
            ui.SetGear(playerCar.GetGear());
            ui.SetStressIndicator(playerCar.GetStress() * 0.5f);
        }



    }
}