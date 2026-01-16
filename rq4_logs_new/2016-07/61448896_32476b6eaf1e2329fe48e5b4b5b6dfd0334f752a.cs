namespace TeamsSuperMan
{
    using System;
    using System.Linq;
    using System.Collections.Generic;

    public class Transport
    {
        private CarList carList;
        private CityTransportCardsList cityCartList;
        private List<ICartable> userCarts;

        public Transport()
        {
            carList = new CarList();
            cityCartList = new CityTransportCardsList();
            userCarts = new List<ICartable>();
        }

        //Rent a Car methods
        public string GetCars(VehiclesType type)
        {
            return carList.GetCars(type);
        }

        public decimal GetPrceOfReservedCar(IVehicles car, DateTime start, DateTime end)
        {
            return carList.GetPrceOfReservedCar(car, start, end);
        }

        // City Transport Carts methods
        public string GetCarts()
        {
            return cityCartList.GetCarts();
        }

        public List<ICartable> GetUserCarts()
        {
            return this.userCarts;
        }

        /// <summary>
        /// Add carts in userCarts
        /// </summary>
        /// <param name="cart">Cart</param>
        /// <param name="countOfCarts">Count of carts thath user wont</param>
        public void AddCart(ICartable cart, int countOfCarts = 1)
        {
            for (int i = 0; i < countOfCarts; i++)
            {
                userCarts.Add(cart);                
            }
        }

        public void DeleteCart(ICartable cart)
        {
            userCarts.Remove(cart);
        }

        public void DeleteAllCarts()
        {
            userCarts.Clear();
        }

        public decimal GetPriceOfUserCarts()
        {
            return userCarts.Sum(x => x.Price);
        }
    }
}