using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DesignPatternPartOne.AbstractFactory
{
    public static class ClientCodeForAbstractFactory
    {
        public static void UseAbstractFactory()
        {
            //Regular vehicle
            IVehicleFactory regularVehicleFactory = new RegularVehicleFactory();
            IBike regularBike = regularVehicleFactory.CreateBike();
            regularBike.GetDetails();
            ICar regularCar = regularVehicleFactory.CreateCar();
            regularCar.GetDetails();

            //Sport Vehicle

            IVehicleFactory sportVehicleFactory = new RegularVehicleFactory();
            IBike sportBike = sportVehicleFactory.CreateBike();
            sportBike.GetDetails();
            ICar sportCar = sportVehicleFactory.CreateCar();
            sportCar.GetDetails();


        }
    }
}