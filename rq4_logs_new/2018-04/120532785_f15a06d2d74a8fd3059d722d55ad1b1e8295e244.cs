using AirlinesManagerGame.Models;
using System;
using System.Collections.ObjectModel;

namespace AirlinesManagerGame.ViewModels
{
    public class AirplaneStoreViewModel : StoreViewModel
    {
        public static ObservableCollection<Airplane> AvailableAirplanesList { get { return store.AvailableAirplanes; } }

        public AirplaneStoreViewModel(User _user) : base(_user)
        {
            user = _user;
        }

        public static bool IsUserHighEnoughLevel(Airplane airplane) { return user.Level >= airplane.LevelToUnlock; }

        public static bool DoesUserHaveTheCapacity() { return user.AvailableAirplaneSlots > 0; }

        public static Airplane CreateNewAirplane(Airplane airplaneType)
        {
            Airplane newAirplane;

            switch (airplaneType.GetType().Name)
            {
                case "Bearclaw":
                    newAirplane = new Bearclaw();
                    break;
                case "Griffon":
                    newAirplane = new Griffon();
                    break;
                case "Wallaby":
                    newAirplane = new Wallaby();
                    break;
                default:
                    throw new Exception();
            }

            newAirplane.LoadType = airplaneType.LoadType;
            return newAirplane;
        }
    }
}