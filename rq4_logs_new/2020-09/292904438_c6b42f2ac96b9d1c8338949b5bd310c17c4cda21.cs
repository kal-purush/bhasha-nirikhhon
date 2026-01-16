
using System.Collections.Generic;

namespace Ex03.GarageLogic
{
    public class ManufectureVehicle
    {
        // Enums:
        public enum eVehicleType
        {
            ElectricBike,
            Bike,
            ElectricCar,
            Car,
            Truck
        }

        // Constructors:
        public static Vehicle CreateVehicle(
            string i_LicenseNumber, 
            string i_Model, 
            List<Wheel> Wheels, 
            Engine i_EngineType, 
            eVehicleType i_VehicleType)
        {
            Vehicle vehicleToCreate = null; // Delete "= null"!!

            switch (i_VehicleType)
            {
                case eVehicleType.ElectricBike:
                    break;
                case eVehicleType.Bike:
                    break;
                case eVehicleType.ElectricCar:
                    break;
                case eVehicleType.Car:
                    break;
                case eVehicleType.Truck:
                    break;
            }

            return vehicleToCreate;
        }
    }
}